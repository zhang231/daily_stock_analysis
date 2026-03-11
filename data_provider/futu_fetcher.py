# -*- coding: utf-8 -*-
"""
===================================
FutuFetcher - 富途牛牛数据源 (Priority 5)
===================================

数据来源：Futu OpenD (富途开放平台)
特点：
- 支持港股、美股、A 股（通过沪深港通）
- 实时行情数据质量高
- 需要配置 Futu OpenD 客户端
- 需要开通相应的市场权限

优先级：5（较低，作为补充数据源）
"""

import logging
import os
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource, safe_float, safe_int

logger = logging.getLogger(__name__)


class FutuFetcher(BaseFetcher):
    """
    Futu OpenD 数据源实现

    优先级：5（补充数据源）
    数据来源：Futu OpenD（富途开放平台）

    支持市场：港股（HK）、A 股（SH/SZ）

    注意：不支持美股（需购买美股行情权限，请使用 YfinanceFetcher）
    使用前请确保 Futu OpenD 已运行且已登录

    设计说明：
    - 使用类级别共享连接（_shared_api），避免重复初始化 OpenD
    - 实例级别只保存配置，实际 API 调用都通过类方法访问共享连接
    """

    name = "FutuFetcher"
    priority = int(os.getenv("FUTA_PRIORITY", "5"))

    # 类级别共享连接
    _shared_api: Optional[Any] = None
    _shared_api_host: Optional[str] = None
    _shared_api_port: Optional[int] = None
    _shared_market_subscribed: set = set()
    _init_lock = False

    def __init__(self):
        """初始化 FutuFetcher（使用共享连接）"""
        self._host = os.getenv("FUTA_OPEND_HOST", "127.0.0.1")
        self._port = int(os.getenv("FUTA_OPEND_PORT", "11111"))
        # 速率限制：每 1 秒最多 1 次请求（满足 30 秒 60 次的限频，更加保守）
        self._min_request_interval = 1.0  # 秒
        self._last_request_time = 0.0

        # 确保共享连接已初始化
        FutuFetcher._ensure_shared_api(self._host, self._port)

    @classmethod
    def _ensure_shared_api(cls, host: str, port: int) -> bool:
        """确保类级别共享 API 已初始化（带简单的并发保护）"""
        # 如果连接已存在且配置匹配，直接复用
        if cls._shared_api is not None:
            if cls._shared_api_host == host and cls._shared_api_port == port:
                return True
            else:
                # 配置变化，关闭旧连接
                logger.info(f"Futu OpenD 配置变化 ({cls._shared_api_host}:{cls._shared_api_port} -> {host}:{port})，重新连接")
                cls._close_shared_api()

        # 检查是否正在初始化（简单的锁机制）
        if cls._init_lock:
            # 等待初始化完成
            import time
            wait_count = 0
            while cls._init_lock and wait_count < 50:  # 最多等 5 秒
                time.sleep(0.1)
                wait_count += 1
            # 等待结束后检查是否初始化成功
            return cls._shared_api is not None

        # 开始初始化
        cls._init_lock = True
        try:
            cls._init_shared_api(host, port)
        finally:
            cls._init_lock = False

        return cls._shared_api is not None

    @classmethod
    def _init_shared_api(cls, host: str, port: int) -> bool:
        """初始化共享 Futu API"""
        try:
            from futu import OpenQuoteContext

            cls._shared_api = OpenQuoteContext(host=host, port=port)
            # 开启异步接收
            cls._shared_api.start()

            # 检查连接状态
            ret, state = cls._shared_api.get_global_state()
            if ret == 0:
                cls._shared_api_host = host
                cls._shared_api_port = port
                logger.info(f"Futu OpenD 共享连接建立成功 ({host}:{port})")
                return True
            else:
                logger.error(f"Futu OpenD 状态异常: {state}")
                cls._close_shared_api()
                return False

        except ImportError:
            logger.warning("Futu API 未安装，请运行：pip install futu-api")
            cls._shared_api = None
            return False
        except Exception as e:
            logger.error(f"Futu OpenD 初始化失败：{e}")
            cls._shared_api = None
            return False

    @classmethod
    def _close_shared_api(cls):
        """关闭共享连接"""
        if cls._shared_api:
            try:
                cls._shared_api.close()
                logger.info("Futu OpenD 共享连接已关闭")
            except Exception as e:
                logger.error(f"关闭 Futu OpenD 连接失败: {e}")
            finally:
                cls._shared_api = None
                cls._shared_api_host = None
                cls._shared_api_port = None
                cls._shared_market_subscribed.clear()

    @property
    def _api(self):
        """访问共享 API 的兼容属性"""
        return FutuFetcher._shared_api

    @classmethod
    def _check_connection(cls) -> bool:
        """检查共享连接是否仍然有效"""
        if cls._shared_api is None:
            return False
        try:
            ret, _ = cls._shared_api.get_global_state()
            return ret == 0
        except Exception:
            return False

    @classmethod
    def _reconnect_if_needed(cls, host: str, port: int) -> bool:
        """如果需要，重新建立连接"""
        if cls._check_connection():
            return True

        logger.warning("Futu OpenD 连接已断开，尝试重新连接...")
        cls._close_shared_api()
        return cls._ensure_shared_api(host, port)

    def _ensure_connection(self) -> bool:
        """实例方法：确保连接可用（自动重连）"""
        return FutuFetcher._reconnect_if_needed(self._host, self._port)
        """访问共享订阅状态的兼容属性"""
        return FutuFetcher._shared_market_subscribed

    def _rate_limit(self):
        """速率限制：确保两次请求之间至少间隔 0.5 秒"""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < self._min_request_interval:
            sleep_time = self._min_request_interval - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def is_available(self) -> bool:
        """检查 Futu 数据源是否可用"""
        return FutuFetcher._shared_api is not None
    
    def _subscribe_market(self, codes: List[str], subtypes: List[Any] = None) -> bool:
        """订阅股票行情"""
        if not self._api or not codes:
            return False

        if subtypes is None:
            from futu import SubType
            subtypes = [SubType.QUOTE, SubType.K_DAY]

        try:
            # 速率限制
            self._rate_limit()

            # 富途 API 的订阅接口使用 code_list 而不是 codes，且不支持 force_refresh 参数
            ret, data = self._api.subscribe(
                code_list=codes,
                subtype_list=subtypes,
                is_first_push=True,
            )
            
            if ret == 0:
                logger.debug(f"Futu 订阅成功: {codes}")
                return True
            else:
                logger.warning(f"Futu 订阅失败 ({codes}): {data}")
                return False
                
        except Exception as e:
            logger.error(f"Futu 订阅异常：{e}")
            return False
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """转换股票代码为 Futu 格式 (MARKET.SYMBOL)"""
        code = stock_code.strip().upper()
        
        # 已经是 Futu 格式 (如 HK.00700, US.AAPL, SH.600519)
        if '.' in code:
            parts = code.split('.')
            if len(parts) == 2:
                if parts[0] == 'US':
                    raise DataFetchError(f"FutuFetcher 不支持美股 {stock_code}，请使用 YfinanceFetcher")
                if parts[0] in ['SH', 'SZ', 'HK']:
                    return code
            # 处理特殊情况 (如 US..SPX)
            if len(parts) == 3 and parts[0] == 'US':
                raise DataFetchError(f"FutuFetcher 不支持美股 {stock_code}，请使用 YfinanceFetcher")
        
        # 处理纯数字代码 (默认 A 股或港股)
        if code.isdigit():
            # 5位及以下通常是港股
            if len(code) <= 5:
                return f"HK.{code.zfill(5)}"
            # 6位是 A 股
            if len(code) == 6:
                if code.startswith(('60', '68')):
                    return f"SH.{code}"
                elif code.startswith(('00', '30')):
                    return f"SZ.{code}"
                elif code.startswith(('51', '58')): # ETF
                    return f"SH.{code}"
                elif code.startswith(('15', '16')): # ETF
                    return f"SZ.{code}"
        
        # 处理纯字母代码 (默认美股)
        if code.isalpha():
            raise DataFetchError(f"FutuFetcher 不支持美股 {stock_code}，请使用 YfinanceFetcher")
        
        # 兼容 HK00700 这种格式
        if code.startswith('HK') and code[2:].isdigit():
            return f"HK.{code[2:].zfill(5)}"
        
        # 兼容 SH600519/SZ000001
        if code.startswith('SH') and code[2:].isdigit():
            return f"SH.{code[2:]}"
        if code.startswith('SZ') and code[2:].isdigit():
            return f"SZ.{code[2:]}"
        
        logger.warning(f"无法自动转换股票代码格式: {stock_code}，尝试原样使用")
        return code
    
    def _get_market(self, futu_code: str) -> str:
        """从 Futu 代码中提取市场"""
        return futu_code.split('.')[0]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从 Futu 获取原始历史数据 (使用 request_history_kline 接口)"""
        # 确保连接可用（自动重连）
        if not self._ensure_connection():
            raise DataFetchError("Futu API 连接失败，请检查 OpenD 是否运行")

        from futu import KLType, AuType, RET_OK
        
        futu_code = self._convert_stock_code(stock_code)
        
        # 对于历史 K 线，通常需要先订阅或者确保有额度
        # request_history_kline 即使没有订阅通常也能拉取，但受限于账号额度
        
        logger.info(f"[Futu] 获取历史 K 线: {futu_code} ({start_date} ~ {end_date})")
        
        try:
            # 速率限制
            self._rate_limit()

            # 使用更准确的接口 request_history_kline (拉取更多历史)
            ret, data, page_req_key = self._api.request_history_kline(
                code=futu_code,
                start=start_date,
                end=end_date,
                ktype=KLType.K_DAY,
                autype=AuType.QFQ,  # 前复权
                max_count=1000
            )

            if ret != RET_OK:
                # 检查是否是额度问题或其他
                raise DataFetchError(f"Futu API 返回错误 ({ret}): {data}")

            if data is None or data.empty:
                raise DataFetchError(f"Futu 未返回 {stock_code} 的数据")

            # 如果有分页，简单的循环处理 (通常 1000 条足够日线)
            all_data = [data]
            while page_req_key is not None:
                # 速率限制：分页请求也需要限流
                self._rate_limit()

                ret, data, page_req_key = self._api.request_history_kline(
                    code=futu_code,
                    start=start_date,
                    end=end_date,
                    ktype=KLType.K_DAY,
                    autype=AuType.QFQ,
                    max_count=1000,
                    page_req_key=page_req_key
                )
                if ret == RET_OK and data is not None and not data.empty:
                    all_data.append(data)
                else:
                    break
            
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"[Futu] 成功获取 {len(final_df)} 条记录")
            return final_df
            
        except Exception as e:
            if isinstance(e, DataFetchError):
                raise
            raise DataFetchError(f"Futu 获取历史数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化 Futu 数据"""
        df = df.copy()
        
        # Futu 返回的列名通常包括: code, time_key, open, close, high, low, volume, turnover, ...
        column_mapping = {
            'time_key': 'date',
            'turnover': 'amount',
        }
        
        df = df.rename(columns=column_mapping)
        
        # 确保日期格式
        if 'date' in df.columns:
            # Futu 的 time_key 可能是 '2023-01-01 00:00:00'
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        # 计算涨跌幅 (如果 API 没给，或者为了统一)
        if 'close' in df.columns:
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0).round(2)
        
        df['code'] = stock_code
        
        # 只保留标准列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """获取实时行情数据"""
        # 确保连接可用（自动重连）
        if not self._ensure_connection():
            logger.debug(f"[Futu] API 连接失败，跳过 {stock_code}")
            return None

        from futu import RET_OK
        
        try:
            futu_code = self._convert_stock_code(stock_code)

            # 实时行情需要先订阅
            from futu import SubType
            self._subscribe_market([futu_code], [SubType.QUOTE])

            # 速率限制
            self._rate_limit()

            ret, data = self._api.get_stock_quote(code_list=[futu_code])
            
            if ret != RET_OK:
                logger.warning(f"[Futu] 获取实时行情失败 ({ret}): {data}")
                return None
            
            if data is None or data.empty:
                logger.warning(f"[Futu] {stock_code} 实时行情数据为空")
                return None
            
            row = data.iloc[0]
            
            # 提取字段
            name = str(row.get('name', ''))
            price = safe_float(row.get('last_price'))
            open_price = safe_float(row.get('open_price'))
            high = safe_float(row.get('high_price'))
            low = safe_float(row.get('low_price'))
            pre_close = safe_float(row.get('prev_close_price'))
            
            change_amount = safe_float(row.get('change'))
            change_pct = safe_float(row.get('change_percent'))
            
            volume = safe_int(row.get('volume'))
            turnover = safe_float(row.get('turnover'))
            
            pe_ratio = safe_float(row.get('pe_ratio'))
            pb_ratio = safe_float(row.get('pb_ratio'))
            total_mv = safe_float(row.get('market_value'))
            
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=name,
                source=RealtimeSource.FUTU, # 使用 FUTU 作为来源标识
                price=price,
                change_pct=change_pct,
                change_amount=change_amount,
                volume=volume,
                amount=turnover,
                volume_ratio=None,
                turnover_rate=None,
                amplitude=None,
                open_price=open_price,
                high=high,
                low=low,
                pre_close=pre_close,
                pe_ratio=pe_ratio,
                pb_ratio=pb_ratio,
                total_mv=total_mv,
                circ_mv=None,
            )
            
            logger.info(f"[Futu] {stock_code} {name}: {price} ({change_pct}%)")
            return quote
            
        except Exception as e:
            logger.error(f"[Futu] 获取 {stock_code} 实时行情异常: {e}")
            return None
    
    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """获取主要指数实时行情"""
        # 确保连接可用（自动重连）
        if not self._ensure_connection():
            return None

        from futu import RET_OK
        
        # 指数映射
        if region == "cn":
            indices_map = {
                'SH.000001': '上证指数',
                'SZ.399001': '深证成指',
                'SZ.399006': '创业板指',
                'SH.000688': '科创 50',
                'SH.000300': '沪深 300',
            }
        elif region == "hk":
            indices_map = {
                'HK.800000': '恒生指数',
                'HK.800100': '恒生科技',
                'HK.800700': '国企指数',
            }
        else:
            return None
        
        try:
            index_codes = list(indices_map.keys())
            # 订阅指数行情
            from futu import SubType
            self._subscribe_market(index_codes, [SubType.QUOTE])

            # 速率限制
            self._rate_limit()

            ret, data = self._api.get_stock_quote(code_list=index_codes)
            
            if ret != RET_OK or data is None or data.empty:
                return None
            
            results = []
            for futu_code, name in indices_map.items():
                row = data[data['code'] == futu_code]
                if row.empty:
                    continue
                
                row = row.iloc[0]
                current = safe_float(row.get('last_price'))
                prev_close = safe_float(row.get('prev_close_price'))
                
                results.append({
                    'code': futu_code.split('.')[1],
                    'name': name,
                    'current': current,
                    'change': safe_float(row.get('change')),
                    'change_pct': safe_float(row.get('change_percent')),
                    'open': safe_float(row.get('open_price')),
                    'high': safe_float(row.get('high_price')),
                    'low': safe_float(row.get('low_price')),
                    'prev_close': prev_close,
                    'volume': safe_int(row.get('volume')),
                    'amount': safe_float(row.get('turnover')),
                })
            
            return results
            
        except Exception as e:
            logger.error(f"[Futu] 获取指数行情异常: {e}")
            return None
    
    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """获取股票名称"""
        # 确保连接可用（自动重连）
        if not self._ensure_connection():
            return None

        from futu import RET_OK
        
        try:
            futu_code = self._convert_stock_code(stock_code)

            # 使用 get_stock_basicinfo 获取股票名称
            # 港股
            if futu_code.startswith('HK'):
                from futu import Market
                # 速率限制
                self._rate_limit()
                ret, data = self._api.get_stock_basicinfo(market=Market.HK, code_list=[futu_code])
            # A 股
            elif futu_code.startswith('SH'):
                from futu import Market
                # 速率限制
                self._rate_limit()
                ret, data = self._api.get_stock_basicinfo(market=Market.SH, code_list=[futu_code])
            elif futu_code.startswith('SZ'):
                from futu import Market
                # 速率限制
                self._rate_limit()
                ret, data = self._api.get_stock_basicinfo(market=Market.SZ, code_list=[futu_code])
            else:
                return None
            
            if ret != RET_OK or data is None or data.empty:
                return None
            
            name = str(data.iloc[0].get('name', ''))
            return name
            
        except Exception as e:
            logger.debug(f"[Futu] 获取 {stock_code} 股票名称失败: {e}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = FutuFetcher()
    
    if not fetcher.is_available():
        print("❌ Futu 数据源不可用，请检查：")
        print("1. 是否安装 futu-api: pip install futu-api")
        print("2. Futu OpenD 是否运行")
        print("3. 配置是否正确：FUTA_OPEND_HOST, FUTA_OPEND_PORT")
    else:
        print("✅ Futu 数据源可用")
        
        print("\n" + "=" * 50)
        print("测试历史数据获取")
        print("=" * 50)
        try:
            df = fetcher.get_daily_data('600519', days=10)
            print(f"[A 股] 获取成功，共 {len(df)} 条数据")
            print(df.tail())
        except Exception as e:
            print(f"[A 股] 获取失败：{e}")
        
        print("\n" + "=" * 50)
        print("测试港股数据获取")
        print("=" * 50)
        try:
            df = fetcher.get_daily_data('00700', days=10)
            print(f"[港股] 获取成功，共 {len(df)} 条数据")
            print(df.tail())
        except Exception as e:
            print(f"[港股] 获取失败：{e}")
        
        print("\n" + "=" * 50)
        print("测试实时行情获取")
        print("=" * 50)
        try:
            quote = fetcher.get_realtime_quote('600519')
            if quote:
                print(f"[实时] {quote.name}: 价格={quote.price}, 涨跌={quote.change_pct}%")
            else:
                print("[实时] 未获取到数据")
        except Exception as e:
            print(f"[实时] 获取失败：{e}")

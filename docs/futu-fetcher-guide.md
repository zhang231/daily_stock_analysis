# FutuFetcher - 富途牛牛数据源使用指南

## 📦 简介

`FutuFetcher` 是基于富途开放平台（Futu OpenD）的数据源实现，提供高质量的港股、美股、A 股实时行情和历史 K 线数据。

**优先级**: 5（补充数据源，在其他数据源失败时使用）

## ✨ 特点

- ✅ **数据质量高**：富途官方数据源，准确可靠
- ✅ **覆盖市场广**：支持港股、美股、A 股（沪深港通）
- ✅ **实时行情**：低延迟的实时报价
- ✅ **复权数据**：支持前复权历史 K 线
- ⚠️ **需要配置**：需运行 Futu OpenD 客户端
- ⚠️ **市场权限**：需要开通相应市场的行情权限

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install futu-api
```

### 2. 下载并运行 Futu OpenD

下载地址：https://www.futunn.com/download/opend

支持平台：
- Windows 10/11
- macOS 10.15+
- Linux (Ubuntu 18.04+)

### 3. 配置环境变量（可选）

在 `.env` 文件中添加：

```bash
# Futu OpenD 配置
FUTA_OPEND_HOST=127.0.0.1      # OpenD 服务地址（默认本地）
FUTA_OPEND_PORT=11111          # OpenD 端口（默认 11111）
FUTA_PRIORITY=5                # 数据源优先级（默认 5）
```

### 4. 使用示例

#### 方式 1：自动使用（推荐）

```python
from data_provider.base import DataFetcherManager

manager = DataFetcherManager()

# 自动在所有数据源间切换，包括 Futu
df, source = manager.get_daily_data('00700')  # 腾讯控股
print(f"数据来源：{source}")
```

#### 方式 2：直接使用

```python
from data_provider.futu_fetcher import FutuFetcher

fetcher = FutuFetcher()

if fetcher.is_available():
    print("✅ Futu 数据源可用")
    
    # 获取历史数据
    df = fetcher.get_daily_data('00700', days=30)
    print(df.tail())
    
    # 获取实时行情
    quote = fetcher.get_realtime_quote('00700')
    print(f"最新价：{quote.price}, 涨跌幅：{quote.change_pct}%")
else:
    print("❌ Futu 数据源不可用")
```

## 📊 支持的股票市场

| 市场 | 代码示例 | Futu 格式 |
|------|---------|----------|
| A 股（沪市） | 600519 | SH.600519 |
| A 股（深市） | 000001 | SZ.000001 |
| 港股 | 00700 | HK.00700 |
| 美股 | AAPL | US.AAPL |
| 美股指数 | SPX | US..SPX |

## 🔧 常见问题

### Q1: Futu OpenD 连接失败

**症状**：`Futu OpenD 连接失败：Connection refused`

**解决方案**：
1. 确认 OpenD 应用程序正在运行
2. 检查端口配置是否正确（默认 11111）
3. 确认防火墙没有阻止连接
4. 检查 `FUTA_OPEND_HOST` 和 `FUTA_OPEND_PORT` 配置

### Q2: 获取数据返回空

**症状**：`Futu 未返回数据`

**可能原因**：
1. 未登录富途账户
2. 未开通对应市场行情权限
3. 股票代码格式错误
4. 日期范围超出限制

**解决方案**：
1. 在 OpenD 客户端登录账户
2. 开通相应市场行情（港股/美股/A 股）
3. 检查股票代码格式
4. 缩短日期范围重试

### Q3: 实时行情延迟

**症状**：行情数据有 15 分钟延迟

**原因**：未开通实时行情权限

**解决方案**：
1. 在富途牛牛中开通实时行情（免费）
2. 确保 OpenD 已登录
3. 某些市场可能需要付费订阅

### Q4: 安装 futu-api 失败

**症状**：`pip install futu-api` 报错

**解决方案**：
```bash
# 升级 pip
pip install --upgrade pip

# 使用国内镜像
pip install futu-api -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## 📈 数据源优先级对比

系统中数据源的优先级顺序（数字越小越优先）：

```
-1. TushareFetcher (配置 Token 时)
 0. EfinanceFetcher
 1. AkshareFetcher
 2. PytdxFetcher / TushareFetcher
 3. BaostockFetcher
 4. YfinanceFetcher
 5. FutuFetcher ← 富途数据源
```

### 调整优先级

可以通过环境变量调整：

```bash
# 设置为最高优先级（优先使用 Futu）
FUTA_PRIORITY=-1

# 保持默认（作为备选）
FUTA_PRIORITY=5
```

## 🎯 最佳实践

1. **作为补充数据源**：建议保持默认优先级 5，在其他数据源失败时使用
2. **港股首选**：对于港股数据，可以临时提升 Futu 优先级
3. **验证数据**：可以用 Futu 验证其他数据源的准确性
4. **监控连接**：定期检查 OpenD 连接状态

## 🔗 参考资料

- [Futu OpenD 官方文档](https://www.futunn.com/download/opend)
- [Futu API 文档](https://openapi.futunn.com/futu-api-doc/)
- [futu-api Python SDK](https://github.com/futuopen/ftapi4py)

---

**注意**：使用 Futu 数据源前，请确保遵守富途牛牛的使用条款和数据许可协议。

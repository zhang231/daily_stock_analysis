# -*- coding: utf-8 -*-
"""
===================================
Obsidian 通知发送器
===================================

职责：
1. 通过 obsidian CLI (URL Scheme) 将分析报告写入 Obsidian 笔记
"""
import logging
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

class ObsidianSender:
    """Obsidian 通知发送器"""

    def __init__(self, config):
        """
        初始化 Obsidian 发送器

        Args:
            config: 配置对象，需包含 obsidian_enabled, obsidian_vault_name, obsidian_daily_note_path
        """
        self._obsidian_enabled = getattr(config, 'obsidian_enabled', False)
        self._vault_name = getattr(config, 'obsidian_vault_name', None)
        self._daily_note_path = getattr(config, 'obsidian_daily_note_path', 'DailyNotes')
        self._cli_path = getattr(config, 'obsidian_cli_path', 'obsidian')

        # 诊断日志
        logger.debug(f"ObsidianSender initialized: enabled={self._obsidian_enabled}, "
                     f"vault={self._vault_name}, path={self._daily_note_path}, cli_path={self._cli_path}")

    def _is_obsidian_configured(self) -> bool:
        """检查 Obsidian 是否已配置且 CLI 可用"""
        if not self._obsidian_enabled or self._vault_name is None:
            return False
        return self._check_obsidian_cli()

    def _check_obsidian_cli(self) -> bool:
        """检查 Obsidian CLI 是否可用"""
        try:
            result = subprocess.run(
                [self._cli_path, "version"],
                capture_output=True,
                timeout=5
            )
            if result.returncode != 0:
                logger.warning(f"⚠️ Obsidian CLI 未安装或不可用 (路径: {self._cli_path})")
                return False
            return True
        except Exception as e:
            logger.warning(f"⚠️ 检查 Obsidian CLI 失败 (路径: {self._cli_path}): {e}")
            return False

    def send_obsidian(self, title: str, content: str) -> bool:
        """
        通过 Obsidian CLI 发送消息 (创建或追加笔记)
        
        Args:
            title: 笔记标题
            content: 笔记内容 (Markdown)
            
        Returns:
            是否发送成功
        """
        if not self._is_obsidian_configured():
            return False

        try:
            # 构造 obsidian create 命令
            # 格式: obsidian create vault=<vault> path=<path> content=<content> overwrite
            file_path = f"{self._daily_note_path}/{title}" if self._daily_note_path else title

            cmd = [
                self._cli_path,
                f"vault={self._vault_name}",
                "create",
                f"path={file_path}",
                f"content={content}",
                "overwrite"
            ]
            

            logger.info(f"正在通过 CLI 创建 Obsidian 笔记: {file_path}")

            # 执行命令: obsidian create vault=... path=... content=... overwrite
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                logger.info(f"Obsidian 笔记指令已发送: {title}")
                return True
            else:
                logger.error(f"Obsidian CLI 调用失败 (code {result.returncode}): {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"发送 Obsidian 通知异常: {e}")
            return False

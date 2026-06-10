"""CLI 子命令定义。每个子命令只构造参数, 实际执行在 main.py 编排。"""
from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gaussdb-archive")
    p.add_argument("--config", required=True, help="archive_config.json 路径")

    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scan", help="扫描 OBS")
    s.add_argument("--cluster", help="指定集群 alias, 不传则扫描所有")

    s = sub.add_parser("pack", help="按天打包")
    s.add_argument("--cluster", required=True)
    s.add_argument("--date", required=True, help="YYYY-MM-DD")

    s = sub.add_parser("archive", help="写入磁带")
    s.add_argument("--full", action="store_true", help="完整流水线: scan→pack→archive")
    s.add_argument("--cluster", help="指定集群")

    s = sub.add_parser("reap", help="安全删除 OBS 原始备份")
    s.add_argument("--cluster", required=True)
    s.add_argument("--date", required=True, help="YYYY-MM-DD")
    s.add_argument("--dry-run", action="store_true")

    s = sub.add_parser("restore-plan", help="生成 PITR 计划")
    s.add_argument("--cluster", required=True)
    s.add_argument("--target", required=True, help="目标时间 YYYY-MM-DD HH:MM:SS")

    s = sub.add_parser("restore", help="执行 PITR 恢复")
    s.add_argument("--cluster", required=True)
    s.add_argument("--target", required=True)
    s.add_argument("--session-id", required=True)

    s = sub.add_parser("cleanup", help="清理恢复数据")
    s.add_argument("--session-id", required=True)

    s = sub.add_parser("status", help="查看状态")
    s.add_argument("--cluster", required=True)
    s.add_argument("--date", help="YYYY-MM-DD, 不传则汇总")

    s = sub.add_parser("cluster", help="集群管理")
    ssub = s.add_subparsers(dest="cluster_command", required=True)
    ssub.add_parser("list")
    ssub.add_parser("show").add_argument("--cluster", required=True)

    return p

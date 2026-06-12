"""CLI 子命令定义。每个子命令只构造参数, 实际执行在 main.py 编排。"""
from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gaussdb-archive")
    p.add_argument("--config", required=True, help="archive_config.json 路径")

    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scan", help="扫描 OBS")
    s.add_argument("--cluster", help="指定集群 alias, 不传则扫描所有")

    # 旧 'pack' 子命令作为 'pack-weekly' 的别名 (向后兼容)
    s = sub.add_parser("pack", help="周度打包 (别名, 同 pack-weekly)")
    s.add_argument("--cluster", required=True)
    s.add_argument("--date", help="已废弃: 用 --week-start 替代")
    s.add_argument("--week-start", help="周起始日 YYYY-MM-DD (不传则取当前周)")
    s.add_argument("--preview", "--dry-run", action="store_true",
                   dest="preview", help="预览模式: 不下载不写盘, 只输出计划清单")

    s = sub.add_parser("pack-weekly", help="周度打包 + 预览")
    s.add_argument("--cluster", required=True)
    s.add_argument("--week-start", help="周起始日 YYYY-MM-DD (不传则取当前周)")
    s.add_argument("--preview", "--dry-run", action="store_true",
                   dest="preview", help="预览模式: 不下载不写盘, 只输出计划清单")

    # 删 'archive' 子命令 (磁带库抽象移除)

    s = sub.add_parser("reap", help="安全删除 OBS 原始备份")
    s.add_argument("--cluster", required=True)
    s.add_argument("--week-start", required=True,
                   help="周起始日 YYYY-MM-DD (reap 整周)")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--yes", action="store_true",
                   help="跳过确认, 直接执行删除 (危险操作)")

    s = sub.add_parser("restore-plan", help="生成 PITR 计划")
    s.add_argument("--cluster", required=True)
    s.add_argument("--target", required=True, help="目标时间 YYYY-MM-DD HH:MM:SS")

    s = sub.add_parser("restore", help="执行 PITR 恢复")
    s.add_argument("--cluster", required=True)
    s.add_argument("--target", required=True)
    s.add_argument("--session-id", required=True)
    s.add_argument("--yes", action="store_true",
                   help="跳过确认, 直接执行恢复")

    s = sub.add_parser("cleanup", help="清理恢复数据")
    s.add_argument("--session-id", required=True)
    s.add_argument("--yes", action="store_true",
                   help="跳过确认, 直接执行清理")

    s = sub.add_parser("status", help="查看状态")
    s.add_argument("--cluster", required=True)
    s.add_argument("--week-start", help="周起始日 YYYY-MM-DD, 不传则汇总")

    s = sub.add_parser("pack-all-weeks", help="逐周打包所有待处理周 (适合首次大规模导入)")
    s.add_argument("--cluster", required=True)
    s.add_argument("--stop-on-error", action="store_true",
                   help="任一周失败即中止 (默认: 记录错误并继续)")

    s = sub.add_parser("cluster", help="集群管理")
    ssub = s.add_subparsers(dest="cluster_command", required=True)
    ssub.add_parser("list")
    ssub.add_parser("show").add_argument("--cluster", required=True)

    return p

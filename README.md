# gaussdb-archive

GaussDB DBS 备份 → OBS → 中间机 → IBM 磁带库 归档与 PITR 恢复系统。

## 文档位置（不在本仓库内）

所有设计文档在 Obsidian 库中：

| 文档 | 路径 | 作用 |
|---|---|---|
| 设计方案 | `30_Areas/00_GaussDB/00_问题处理/01_维护经验/06_GaussDB_OBS_备份转储磁带库_设计方案.md` | 整体架构、模块设计、关键流程 |
| 参考文档 | `30_Areas/00_GaussDB/00_问题处理/01_维护经验/05_GaussDB_OBS_备份目录结构说明.md` | GaussDB OBS 目录结构真实样本 |
| 实施计划 | `30_Areas/00_GaussDB/00_问题处理/01_维护经验/2026-06-10-GaussDB_OBS_备份归档至磁带库_实施计划.md` | 本仓库代码的 TDD 任务拆解 |

> 实施计划是**代码层的真相源**，设计稿是**架构层的真相源**。两者冲突时以设计稿为准并修订实施计划。

## 集群示例 (与设计稿 1.4.1 对齐)

| alias | instance_id (OBS 目录名) | display_name | 策略 |
|---|---|---|---|
| `ncbs_busi` | `2c61167d2f1f42858bc2a719a1275eae_gbb87e99b3215e2ccc8e5f4779ecf715in08` | 核心数据库集群 | full+snapshot+diff+xlog |
| `trgl_busi` | `2c61167d2f1f42858bc2a719a1275eae_faa76d88a2104d1bbb7d4e3668dbe612in14` | 总账数据库集群 | full+snapshot+diff+xlog |
| `itps_busi` | `2c61167d2f1f42858bc2a719a1275eae_4e83389f4h3h64070de4c931c3497gcg2005` | 柜面数据库集群 | full+snapshot+diff (无 xlog) |

> ⚠️ `instance_id` 必须是**完整**的 `{tenant_id}_{instance_id}` 字符串 (来自 OBS 实际目录名),
> 不可简化为别名。`alias` 才是简称 (如 `ncbs_busi`)。

## 快速开始

参见实施计划 Task 1 起的"运行"小节。

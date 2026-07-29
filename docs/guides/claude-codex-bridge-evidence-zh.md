# Claude Code → Codex OAuth 协议失败证据指南

本文说明 CC Switch 如何记录 Claude Code 到内置 Codex OAuth 后端之间无法正确转换的协议请求和响应，以及如何安全地检查、导出、删除和离线重放这些证据。

## 适用范围

证据采集只在以下条件同时成立时启用：

- 客户端是 Claude Code；
- 供应商是内置的 `codex_oauth`；
- API 格式是 `openai_responses`。

其他应用、第三方 Responses 网关和反向的 Codex → Claude 流量不在本阶段范围内。工具仍由 Claude Code 在本地执行；CC Switch 只负责协议转换和取证，不会代替 Claude Code 读取文件、执行命令或调用 MCP。

## 什么时候会保存证据

每次符合范围的请求都会先在临时目录中记录转换前后的协议数据。正常完成后，临时记录立即删除，不留下持久证据。以下失败会提交为证据包：

- Claude 请求无法转换成 Codex Responses 请求；
- Codex 上游拒绝请求或返回失败状态；
- 非流式 Codex 响应无法转换回 Claude 响应；
- 流式事件非法、明确失败，或连接在终止事件前中断。

日志使用固定的安全摘要，并包含 `bundle_id`、失败阶段和错误分类，便于定位对应证据包。流式证据按 NDJSON 逐事件追加，代理转发的数据不会因观察器而改变。

## 保存位置和结构

默认证据根目录是：

```text
~/.cc-switch/bridge-evidence/
├── staging/    # 当前请求的临时记录
└── bundles/    # 已提交的失败证据包
```

如果 CC Switch 配置了自定义应用数据目录，根目录随之变为 `<自定义应用数据目录>/bridge-evidence`。每个 `bundles/<bundle_id>/` 至少包含 `manifest.json`；完整证据还可包含转换前后的 JSON 或 NDJSON 文件。清单记录文件名、长度和 SHA-256，重放和导出时会校验。

保留策略为 7 天、总计最多 200 MiB。清理时先删除过期包，再从最旧的包开始删除，直至满足容量限制。Unix 上目录和文件分别使用仅当前用户可访问的 `0700`/`0600` 权限；Windows 继承当前用户应用配置目录的 ACL。

## 安全边界

保存前会递归移除认证头、API key、OAuth token、Cookie、密码、账户标识等凭据。正文中出现 `Bearer`、常见 API key 前缀或 JWT 形态的字符串时，也会触发 fail-closed：CC Switch 不保存完整协议正文，只提交一份说明“完整采集已被抑制”的结构性记录。

脱敏不等于匿名化。没有触发抑制的完整失败包可能仍包含提示词、源码、工具参数和工具输出，应按敏感本地数据处理：

- 不会自动上传或发送给任何服务；
- 导出前应人工检查内容；
- 不要把证据包提交到 Git；
- 问题分析完成后应及时删除。

## 管理接口

Stage 0 提供四个 Tauri 命令，供后续诊断界面或开发者控制台调用：

| 命令 | 参数 | 作用 |
| --- | --- | --- |
| `list_bridge_evidence` | 无 | 按包列出时间、供应商、模型、阶段、错误类型、大小和是否为完整采集 |
| `export_bridge_evidence` | `bundleId`, `destination` | 把一个包导出为 ZIP；目标必须在证据根目录之外 |
| `delete_bridge_evidence` | `bundleId` | 永久删除指定证据包 |
| `cleanup_bridge_evidence` | 无 | 立即执行 7 天/200 MiB 保留策略并返回清理统计 |

这些接口严格校验证据包 ID 和路径。ZIP 只包含清单明确列出的普通文件，不跟随符号链接，也不允许路径穿越。

## 离线重放

在仓库的 `src-tauri` 目录运行：

```powershell
cargo run --example replay_bridge_bundle -- <证据包目录>
```

例如验证仓库内置的非流式工具调用样例：

```powershell
cargo run --example replay_bridge_bundle -- tests/fixtures/bridge-forensics/non-stream-tool-call
```

重放器先校验清单版本、文件白名单、文件长度和 SHA-256，再通过当前请求/响应转换器重跑数据。它不启动代理、不读取 OAuth 凭据，也不会发出网络请求。报告中的 `network_requests` 固定为 `0`；不一致时只输出 JSON 路径、预期类型、实际类型和原因，不复制原始字段值到终端。

当前重放器支持：

- 非流式 JSON 响应；
- Responses SSE 事件对应的 NDJSON 流式证据；
- 请求侧 Codex 结构和响应侧 Claude 结构的确定性比较。

## 建议的问题分析流程

1. 从 `[BridgeEvidence]` 日志取得 `bundle_id` 和失败阶段。
2. 用 `list_bridge_evidence` 确认包是否存在、是否因安全原因只保留了结构记录。
3. 在本机检查 `manifest.json`；只有确有需要时才查看完整协议文件。
4. 对完整包运行离线重放，记录结构差异。
5. 需要交给开发者人工比对时，先检查内容，再导出到证据目录之外。
6. 完成分析后调用 `delete_bridge_evidence`；也可调用 `cleanup_bridge_evidence` 立即执行保留策略。

Stage 0 的目的只是建立可靠的失败反馈环。能力协商、请求级工具注册表、会话账本和严格流式状态机将在后续阶段实现；当前证据系统不表示旧协议转换已经具备这些 Agent 语义。

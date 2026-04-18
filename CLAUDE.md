# CLAUDE.md — adortb-dmp

本文件为 Claude Code 在 adortb-dmp 项目中的工作指引。

---

## 项目概览

adortb-dmp 是 AdortB 广告系统的 DMP（Data Management Platform）服务，使用 **Go 1.25.3** 编写，**零外部依赖**，完全基于 Go 标准库实现。

- HTTP 服务端口：`8086`（`LISTEN_ADDR` 环境变量）
- 核心存储：Redis（连接池 50）+ ClickHouse（HTTP API）
- 消息消费：Kafka Wire Protocol，Consumer Group `adortb-dmp`

详细架构见 [docs/architecture.md](docs/architecture.md)，项目概述见 [README.md](README.md)。

---

## 技术约束

### Go 版本

使用 **Go 1.25.3**，所有代码必须符合该版本语法。编写代码前先确认 `go.mod` 中的 `go` 指令版本。

### 零外部依赖

本项目不引入任何第三方库。所有功能均通过标准库实现：

- HTTP：`net/http`
- Redis：`net` + 手写 RESP 协议
- ClickHouse：`net/http`（HTTP API）
- Kafka：`net` + 手写 Wire Protocol
- JSON：`encoding/json`
- 指标：`net/http` 暴露 Prometheus 文本格式

**禁止**在 `go.mod` 中添加任何 `require` 依赖。

---

## 目录结构与职责

```
adortb-dmp/
├── cmd/dmp/main.go              # 程序入口，只做环境变量读取和服务组装
├── internal/
│   ├── api/server.go            # HTTP 路由（路由注册，不含业务逻辑）
│   ├── api/processor.go         # 行为处理管道（验证→打标→存储，串联各层）
│   ├── behavior/event.go        # 事件类型定义与合法性校验
│   ├── behavior/handler.go      # 行为处理器（单条事件处理入口）
│   ├── tagging/engine.go        # 标签规则引擎（并发执行规则）
│   ├── tagging/rule.go          # 10 条内置标签规则（禁止在此文件外增加规则）
│   ├── store/redis.go           # Redis 存储层（Key 设计见下方）
│   ├── store/clickhouse.go      # ClickHouse 批量写入（100 条/5s 双触发）
│   ├── lookup/service.go        # 标签批量查询（P99<10ms，≤200 用户/次）
│   ├── expand/service.go        # 受众扩展（基于标签交集排序）
│   ├── consumer/kafka.go        # Kafka 消费（多 topic 并发，5s 自动重连）
│   └── metrics/metrics.go       # Prometheus 指标（文本格式）
└── client/client.go             # Go 客户端库（DSP 侧调用）
```

### 文件大小限制

- 单文件不超过 **800 行**
- 函数体不超过 **50 行**
- 超出时必须拆分为多个文件，按功能/子域组织

---

## Redis Key 设计规范

修改存储层时，必须严格遵守以下 Key 规范：

| Key 模式 | 类型 | 用途 | TTL |
|----------|------|------|-----|
| `user:tags:{uid}` | HASH | 用户标签及分值 | 30 天 |
| `tag:users:{tag_id}` | ZSET | 标签下用户集合（score 排序） | 30 天 |
| `user:evcnt:{uid}:*` | String | 用户行为事件计数（按事件类型分 key） | 8 天 |

**禁止**未经评审擅自新增 Key 模式。

---

## 性能要求

| 场景 | 目标 |
|------|------|
| 批量标签查询（≤200 用户） | P99 < 10ms |
| ClickHouse 写入批次 | 100 条/批 或 5 秒触发 |
| Kafka 断线重连 | 5 秒间隔 |
| Redis 连接池 | 50 个连接 |

编写涉及热路径的代码时必须：

1. 避免不必要的内存分配（复用 buffer、使用 `sync.Pool`）
2. 减小锁粒度（分片锁、读写锁优先于互斥锁）
3. 批量操作优先于逐条操作（Pipeline、批量 MGET）
4. 避免在持锁区间执行 I/O

---

## 并发安全

- 所有共享状态必须通过锁或 channel 保护
- 禁止裸写共享变量（不使用 `sync/atomic` 或锁的情况下）
- 标签引擎并发执行多条规则时，每条规则的输出必须独立（无共享写入）
- Kafka 消费 goroutine 与 HTTP handler goroutine 之间通过 channel 解耦

---

## 错误处理

- 禁止 `panic`（除 `main` 初始化阶段不可恢复的配置错误外）
- 所有错误必须显式处理，不得 `_` 丢弃
- 对外 HTTP 响应的错误信息不得暴露内部细节（使用通用错误码+消息）
- 服务端日志记录完整错误上下文

---

## 单元测试要求

- 每个新功能/修改必须附带对应单元测试
- 测试覆盖率目标：**80%+**
- 测试文件放置规则：与被测文件同目录，命名为 `*_test.go`
- 使用标准库 `testing` 包，禁止引入测试框架（如 testify）
- 关键路径（lookup、tagging engine）必须包含 benchmark 测试

### 测试命令

```bash
# 运行所有测试
go test ./...

# 带覆盖率
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# benchmark
go test -bench=. -benchmem ./internal/lookup/...
```

---

## HTTP 端点一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/behavior` | 上报行为事件 |
| GET  | `/v1/user/{user_id}/tags` | 用户标签查询 |
| GET  | `/v1/user/{user_id}/profile` | 用户画像 |
| POST | `/v1/tags/batch` | 批量标签查询（≤200 用户） |
| GET  | `/v1/audience/{tag_id}/users` | 标签用户集合 |
| POST | `/v1/audience/expand` | 受众相似扩展 |
| GET  | `/health` | 健康检查 |
| GET  | `/metrics` | Prometheus 指标 |

---

## 环境变量

```
LISTEN_ADDR=:8086
REDIS_ADDR=localhost:6379
CLICKHOUSE_HTTP=http://localhost:8123
KAFKA_BROKER=localhost:9092
KAFKA_TOPICS=adortb.events,adortb.behaviors
```

`main.go` 中通过 `os.Getenv` 读取，缺失时使用默认值，**不得** hardcode 在业务代码中。

---

## 常见开发任务

### 新增标签规则

1. 在 `internal/tagging/rule.go` 中添加新规则（实现 `Rule` 接口）
2. 在 `internal/tagging/engine.go` 的规则列表中注册
3. 编写对应单元测试

### 新增 HTTP 端点

1. 在 `internal/api/server.go` 注册路由
2. 在对应的 service 层实现业务逻辑（不在 handler 中写业务）
3. 编写单元测试和集成测试

### 修改 Redis 存储结构

1. 更新 `internal/store/redis.go`
2. 更新本文件中的 Key 设计表格
3. 更新 [docs/architecture.md](docs/architecture.md) 中的存储层说明

---

## 禁止事项

- 禁止引入外部依赖（go.mod 不得出现 require 块）
- 禁止在 API handler 中直接操作 Redis/ClickHouse（必须通过 store 层）
- 禁止跨层直接调用（如 tagging 直接调用 consumer）
- 禁止在 rule.go 以外的地方硬编码标签规则逻辑
- 禁止忽略 context 取消信号（所有 I/O 调用必须支持 context）

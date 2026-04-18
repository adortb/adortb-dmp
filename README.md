# adortb-dmp

**DMP（Data Management Platform）** —— AdortB 广告系统的用户数据管理平台，负责行为事件采集、用户标签打标、受众画像构建与实时标签查询，为 DSP 竞价决策提供毫秒级的用户特征支撑。

---

## 整体架构

```
              ┌─────────────────────────────────────────┐
              │     Web SDK / iOS / Android / CTV       │
              └────────────────┬────────────────────────┘
                               ↓
                       ┌───────────────┐
                       │   ADX Core    │◀──外部 DSP─┐
                       └───────┬───────┘            │
                   ┌───────────┼───────────┐        │
                   ↓           ↓           ↓        │
              ┌────────┐ ┌────────┐ ┌────────┐     │
              │  DSP   │ │  MMP   │ │  SSAI  │─────┘
              └───┬────┘ └───┬────┘ └────────┘
                  ↓          ↓
              ┌───────────────────────────┐
              │  Event Pipeline (Kafka)   │
              └───────┬───────────────────┘
        ┌─────────────┼─────────────┐
        ↓             ↓             ↓
  ┌─────────┐   ┌──────────┐   ┌──────────┐
  │ Billing │   │  ★DMP    │   │   CDP    │
  └─────────┘   └──────────┘   └──────────┘
                       ↓
                  ┌──────────┐
                  │  Admin   │◀── Frontend
                  └──────────┘
```

DMP 处于 AdortB 系统的核心数据枢纽位置：

- **上游**：从 Kafka Event Pipeline 消费行为事件；App SDK 通过 HTTP 直接上报
- **下游**：DSP 通过 Go client 库批量查询标签参与竞价；CDP 消费 `adortb.dmp.tags` topic
- **Admin**：通过 Admin 服务对接前端进行运营管理

---

## 技术栈

| 层次 | 技术 | 说明 |
|------|------|------|
| 语言 | Go 1.25.3 | 零外部依赖，完全使用标准库 |
| HTTP | net/http | 监听 `:8086`（LISTEN_ADDR） |
| 缓存/标签存储 | Redis | 标准库 TCP，连接池 50 |
| 事件归档 | ClickHouse | HTTP API，批量写入 100 条/5 秒 |
| 消息消费 | Kafka | Wire Protocol，Consumer Group: `adortb-dmp` |

---

## 目录结构

```
adortb-dmp/
├── cmd/dmp/main.go              # 程序入口，环境变量读取与服务启动
├── internal/
│   ├── api/server.go            # HTTP 路由注册
│   ├── api/processor.go         # 行为处理管道（验证→打标→存储）
│   ├── behavior/event.go        # 事件类型定义与校验
│   ├── behavior/handler.go      # 行为处理器
│   ├── tagging/engine.go        # 标签规则引擎
│   ├── tagging/rule.go          # 10 条内置标签规则
│   ├── store/redis.go           # Redis 存储层（标签/计数/用户集合）
│   ├── store/clickhouse.go      # ClickHouse 批量写入层
│   ├── lookup/service.go        # 标签查询服务（批量 P99<10ms）
│   ├── expand/service.go        # 受众相似扩展
│   ├── consumer/kafka.go        # Kafka 多 topic 并发消费
│   └── metrics/metrics.go       # Prometheus 指标暴露
└── client/client.go             # Go 客户端库（供 DSP 调用）
```

---

## HTTP 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/behavior` | 上报行为事件 |
| GET  | `/v1/user/{user_id}/tags` | 查询用户标签列表 |
| GET  | `/v1/user/{user_id}/profile` | 查询用户完整画像 |
| POST | `/v1/tags/batch` | 批量查询标签（≤200 用户，P99<10ms） |
| GET  | `/v1/audience/{tag_id}/users` | 查询某标签下的用户集合 |
| POST | `/v1/audience/expand` | 受众相似扩展（种子标签 → 扩展用户） |
| GET  | `/health` | 健康检查 |
| GET  | `/metrics` | Prometheus 指标 |

---

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LISTEN_ADDR` | `:8086` | HTTP 监听地址 |
| `REDIS_ADDR` | `localhost:6379` | Redis 地址 |
| `CLICKHOUSE_HTTP` | `http://localhost:8123` | ClickHouse HTTP API 地址 |
| `KAFKA_BROKER` | `localhost:9092` | Kafka Broker 地址 |
| `KAFKA_TOPICS` | `adortb.events,adortb.behaviors` | 消费的 Kafka topic 列表 |

---

## 内置标签规则

DMP 内置 10 条标签规则，覆盖用户兴趣、购买意向、活跃度和设备特征等维度，规则由 `internal/tagging/rule.go` 定义，由 `internal/tagging/engine.go` 驱动执行。

---

## Redis Key 设计

| Key 模式 | 类型 | 说明 | TTL |
|----------|------|------|-----|
| `user:tags:{uid}` | HASH | 用户标签及分值 | 30 天 |
| `tag:users:{tag_id}` | ZSET | 标签下的用户集合（按分值排序） | 30 天 |
| `user:evcnt:{uid}:*` | String | 用户行为事件计数 | 8 天 |

---

## 性能指标

- 批量标签查询（≤200 用户）P99 延迟 < 10ms
- ClickHouse 批量写入：100 条/批 或 5 秒触发一次
- Kafka 自动重连间隔：5 秒

---

## 快速启动

```bash
# 设置环境变量
export LISTEN_ADDR=:8086
export REDIS_ADDR=localhost:6379
export CLICKHOUSE_HTTP=http://localhost:8123
export KAFKA_BROKER=localhost:9092
export KAFKA_TOPICS=adortb.events,adortb.behaviors

# 编译并运行
go build -o dmp ./cmd/dmp
./dmp
```

---

## DSP 客户端使用

```go
import "github.com/adortb/adortb-dmp/client"

c := client.New("http://dmp-service:8086")

// 批量查询用户标签
tags, err := c.BatchTags(ctx, []string{"user1", "user2", "user3"})
```

详见 `client/client.go`。

---

## 相关文档

- [架构设计](docs/architecture.md) —— 内部架构、数据流、时序图

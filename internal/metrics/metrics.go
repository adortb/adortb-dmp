package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Counters 全局计数器（无外部依赖的简单 Prometheus 文本格式）
type Counters struct {
	BehaviorReceived  atomic.Int64
	BehaviorProcessed atomic.Int64
	BehaviorFailed    atomic.Int64
	TagsApplied       atomic.Int64
	LookupRequests    atomic.Int64
	KafkaConsumed     atomic.Int64
}

var Global = &Counters{}

// Handler 返回 Prometheus 文本格式的 metrics
func Handler(c *Counters) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP dmp_behavior_received_total 接收到的行为事件总数\n")
		fmt.Fprintf(w, "# TYPE dmp_behavior_received_total counter\n")
		fmt.Fprintf(w, "dmp_behavior_received_total %d\n", c.BehaviorReceived.Load())

		fmt.Fprintf(w, "# HELP dmp_behavior_processed_total 已处理的行为事件总数\n")
		fmt.Fprintf(w, "# TYPE dmp_behavior_processed_total counter\n")
		fmt.Fprintf(w, "dmp_behavior_processed_total %d\n", c.BehaviorProcessed.Load())

		fmt.Fprintf(w, "# HELP dmp_behavior_failed_total 处理失败的行为事件总数\n")
		fmt.Fprintf(w, "# TYPE dmp_behavior_failed_total counter\n")
		fmt.Fprintf(w, "dmp_behavior_failed_total %d\n", c.BehaviorFailed.Load())

		fmt.Fprintf(w, "# HELP dmp_tags_applied_total 打标签操作总数\n")
		fmt.Fprintf(w, "# TYPE dmp_tags_applied_total counter\n")
		fmt.Fprintf(w, "dmp_tags_applied_total %d\n", c.TagsApplied.Load())

		fmt.Fprintf(w, "# HELP dmp_lookup_requests_total 标签查询请求总数\n")
		fmt.Fprintf(w, "# TYPE dmp_lookup_requests_total counter\n")
		fmt.Fprintf(w, "dmp_lookup_requests_total %d\n", c.LookupRequests.Load())

		fmt.Fprintf(w, "# HELP dmp_kafka_consumed_total Kafka消费消息总数\n")
		fmt.Fprintf(w, "# TYPE dmp_kafka_consumed_total counter\n")
		fmt.Fprintf(w, "dmp_kafka_consumed_total %d\n", c.KafkaConsumed.Load())
	}
}

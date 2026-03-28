package mock

import (
	"fmt"

	"github.com/chahatsagarmain/GoStream/internal/memstore"
)

// PopulateStore injects dummy topics, consumers, and messages directly into the
// in-memory store so the message service is immediately hydrated on boot.
func PopulateStore() {
	topicNames := []string{"system-logs", "user-events", "sensor-data"}

	for _, t := range topicNames {
		// 1. Create a mock topic
		memstore.CreateTopic(t)

		// 2. Create a mock consumer reading this topic
		consumerName := fmt.Sprintf("mock-consumer-%s", t)
		memstore.CreateConsumer(consumerName, t)

		// 3. Publish mock messages into the topic stream
		for i := 1; i <= 10; i++ {
			msg := fmt.Sprintf("Mock payload #%d securely transmitted via %s queue", i, t)
			memstore.AppendToLog(t, msg)
		}
	}
}

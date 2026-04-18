package memstore

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestCreateTopic(t *testing.T) {
	Reset()

	err := CreateTopic("topic1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	topics := GetTopics()
	if len(topics) != 1 || topics[0] != "topic1" {
		t.Fatalf("expected [topic1], got %v", topics)
	}

	// Test creating existing topic (should ignore)
	err = CreateTopic("topic1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	topics = GetTopics()
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
}

func TestDeleteTopic(t *testing.T) {
	Reset()

	CreateTopic("topic1")
	CreateConsumer("consumer1", "topic1")
	AppendToLog("topic1", "msg1")

	topics := GetTopics()
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	err := DeleteTopic("topic1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	topics = GetTopics()
	if len(topics) != 0 {
		t.Fatalf("expected 0 topics, got %d", len(topics))
	}

	// Make sure messages are deleted
	msgs := GetAllMessages()
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages left, got %d", len(msgs))
	}

	// Make sure offsets are deleted
	offs := GetAllOffsets()
	if len(offs) != 0 {
		t.Fatalf("expected 0 offsets left, got %d", len(offs))
	}
}

func TestCreateConsumer(t *testing.T) {
	Reset()

	// Topic doesn't exist
	err := CreateConsumer("consumer1", "topic1")
	if err == nil {
		t.Fatal("expected error creating consumer for non-existent topic, got nil")
	}

	CreateTopic("topic1")
	err = CreateConsumer("consumer1", "topic1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	consumers := GetConsumers()
	if len(consumers) != 1 || consumers[0] != "consumer1" {
		t.Fatalf("expected [consumer1], got %v", consumers)
	}

	offset, err := GetOffset("consumer1", "topic1")
	if err != nil || offset != 0 {
		t.Fatalf("expected offset 0, got %v with string err %v", offset, err)
	}
}

func TestDeleteConsumer(t *testing.T) {
	Reset()

	CreateTopic("topic1")
	CreateConsumer("consumer1", "topic1")
	SetOffset("consumer1", "topic1", 5)

	err := DeleteConsumer("consumer1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	offs := GetAllOffsets()
	if len(offs) != 0 {
		t.Fatalf("expected 0 offsets left, got %v", offs)
	}

	consumers := GetConsumers()
	if len(consumers) != 0 {
		t.Fatalf("expected 0 consumers left, got %v", consumers)
	}
}

func TestAppendAndGetMessage(t *testing.T) {
	Reset()

	CreateTopic("topic1")
	CreateConsumer("consumer1", "topic1")

	// Empty log
	_, err := GetMessageFromLog("consumer1", "topic1")
	if err == nil {
		t.Fatal("expected error getting message from empty log, got nil")
	}

	AppendToLog("topic1", "hello")
	AppendToLog("topic1", "world")

	msg, err := GetMessageFromLog("consumer1", "topic1")
	if err != nil || msg != "hello" {
		t.Fatalf("expected hello, got %s with err %v", msg, err)
	}

	msg, err = GetMessageFromLog("consumer1", "topic1")
	if err != nil || msg != "world" {
		t.Fatalf("expected world, got %s with err %v", msg, err)
	}

	// Should be out of bounds now
	_, err = GetMessageFromLog("consumer1", "topic1")
	if err == nil {
		t.Fatal("expected error getting message out of bounds, got nil")
	}

	// Offset should have moved to 2
	offset, _ := GetOffset("consumer1", "topic1")
	if offset != 2 {
		t.Fatalf("expected offset 2, got %d", offset)
	}
}

func TestSetAndGetOffset(t *testing.T) {
	Reset()

	CreateTopic("t1")
	CreateConsumer("c1", "t1")

	err := SetOffset("c1", "t1", 42)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	off, err := GetOffset("c1", "t1")
	if err != nil || off != 42 {
		t.Fatalf("expected 42, got %d with err %v", off, err)
	}

	// Test non-existent topic
	err = SetOffset("c1", "tunknown", 10)
	if err == nil {
		t.Fatal("expected error for non-existent topic")
	}
}

func TestRestoreStore(t *testing.T) {
	Reset()

	topics := []string{"t1", "t2"}
	consumers := []string{"c1"}
	messages := map[string][]string{
		"t1": {"m1", "m2"},
	}
	offsets := map[string]int{
		"t1:c1": 2,
	}

	RestoreStore(topics, consumers, messages, offsets)

	tops := GetTopics()
	if len(tops) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(tops))
	}

	cons := GetConsumers()
	if len(cons) != 1 || cons[0] != "c1" {
		t.Fatalf("expected [c1], got %v", cons)
	}

	allMsgs := GetAllMessages()
	if !reflect.DeepEqual(allMsgs, messages) {
		t.Fatalf("expected %v, got %v", messages, allMsgs)
	}

	allOffs := GetAllOffsets()
	if !reflect.DeepEqual(allOffs, offsets) {
		t.Fatalf("expected %v, got %v", offsets, allOffs)
	}
}

func BenchmarkCreateTopic(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CreateTopic("topic-" + strconv.Itoa(i))
	}
}

func BenchmarkAppendToLog(b *testing.B) {
	topic := "bench-topic-append"
	CreateTopic(topic)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AppendToLog(topic, "test message")
	}
}

func BenchmarkAppendToLogParallel(b *testing.B) {
	topic := "bench-topic-append-parallel"
	CreateTopic(topic)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			AppendToLog(topic, "test message")
		}
	})
}

func BenchmarkGetMessageFromLog(b *testing.B) {
	topic := "bench-topic-get"
	consumer := "bench-consumer-get"
	CreateTopic(topic)
	CreateConsumer(consumer, topic)

	// Keep adding messages so we don't hit offset bounds
	for i := 0; i < b.N; i++ {
		AppendToLog(topic, "test message")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := GetMessageFromLog(consumer, topic)
		if err != nil {
			b.Fatalf("failed to get message: %v", err)
		}
	}
}

func BenchmarkGetMessageFromLogParallel(b *testing.B) {
	topic := "bench-topic-get-parallel"
	CreateTopic(topic)

	for i := 0; i < b.N; i++ {
		AppendToLog(topic, "test message")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Create a separate consumer for each goroutine to avoid race
		// condition on reading the same offset simultaneously causing
		// "offset out of bounds" errors if incremented ungracefully
		consumer := "consumer-parallel-" + strconv.Itoa(rand.Int())
		CreateConsumer(consumer, topic)

		for pb.Next() {
			_, err := GetMessageFromLog(consumer, topic)
			if err != nil {
				// To prevent parallel aborts, just break or continue
				continue
			}
		}
	})
}

func BenchmarkGetTopics(b *testing.B) {
	for i := 0; i < 1000; i++ {
		CreateTopic(fmt.Sprintf("topic-get-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetTopics()
	}
}

// Capacity benchmarks focus on scaling sizes

// BenchmarkCapacity_10kTopics demonstrates creating a large number of topics.
// Due to the O(n) check in CreateTopic, this might scale poorly.
func BenchmarkCapacity_10kTopics(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Because there is no Reset function in memstore, we recreate unique names
		// to test the growing slice, or we can just measure insertion time as it grows.
		// Doing it 10,000 times per b.N will be very slow for large N due to O(N^2)
		// We'll reset timer for each iteration manually
		b.StopTimer()
		prefix := fmt.Sprintf("cap10k-%d-", i)
		b.StartTimer()

		for j := 0; j < 10000; j++ {
			CreateTopic(prefix + strconv.Itoa(j))
		}
	}
}

// BenchmarkCapacity_100kMessages tests publishing a large number of messages to a single topic
func BenchmarkCapacity_100kMessages(b *testing.B) {
	topic := "capacity-topic-100k-msgs"
	CreateTopic(topic)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// publish 100k messages to a single topic
		for j := 0; j < 100000; j++ {
			AppendToLog(topic, "capacity test payload data")
		}
	}
}

// BenchmarkCapacity_10kConsumers tests creating many consumers on a single topic
func BenchmarkCapacity_10kConsumers(b *testing.B) {
	topic := "capacity-topic-10k-consumers"
	CreateTopic(topic)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		prefix := fmt.Sprintf("cons10k-%d-", i)
		b.StartTimer()

		for j := 0; j < 10000; j++ {
			CreateConsumer(prefix+strconv.Itoa(j), topic)
		}
	}
}

// BenchmarkCapacity_MixedWorkload tests a realistic mixed workload
// of creating a large number of topics and immediately appending messages to them.
func BenchmarkCapacity_MixedWorkload(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		prefix := fmt.Sprintf("mix-%d-", i)
		b.StartTimer()

		// System creates 1,000 topics and inserts 100 messages into each
		for j := 0; j < 1000; j++ {
			topic := prefix + strconv.Itoa(j)
			CreateTopic(topic)
			for k := 0; k < 100; k++ {
				AppendToLog(topic, "mixed workload message")
			}
		}
	}
}

// BenchmarkCapacity_FullMix tests a fully mixed workload:
// Creating topics, creating consumers, publishing messages, and consuming them.
func BenchmarkCapacity_FullMix(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		prefix := fmt.Sprintf("full-%d-", i)
		b.StartTimer()

		// 100 topics, 10 consumers per topic, 100 msgs published & consumed per consumer
		for j := 0; j < 100; j++ {
			topic := prefix + strconv.Itoa(j)
			CreateTopic(topic)

			// Create 10 consumers per topic
			var consumers []string
			for c := 0; c < 10; c++ {
				consumer := "consumer-" + strconv.Itoa(c)
				CreateConsumer(consumer, topic)
				consumers = append(consumers, consumer)
			}

			// Publish 100 messages
			for k := 0; k < 100; k++ {
				AppendToLog(topic, "full mixed workload message payload")
			}

			// Consume messages across all consumers
			for _, consumer := range consumers {
				for m := 0; m < 100; m++ {
					_, err := GetMessageFromLog(consumer, topic)
					if err != nil {
						b.Fatalf("failed to consume message: %v", err)
					}
				}
			}
		}
	}
}

// Reset clears all state, useful for testing
func Reset() {
	topics.Lock()
	defer topics.Unlock()
	consumers.Lock()
	defer consumers.Unlock()
	messages.Lock()
	defer messages.Unlock()
	offsets.Lock()
	defer offsets.Unlock()

	topics.items = make(map[string]int)
	consumers.items = make(map[string]int)
	messages.logs = make(map[string][]string)
	offsets.positions = make(map[string]int)
}

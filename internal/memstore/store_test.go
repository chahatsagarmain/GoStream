package memstore

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/chahatsagarmain/GoStream/config"
	"github.com/chahatsagarmain/GoStream/internal/memstore/types"
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

func init() {
	// Configure large segment sizes for benchmarks so they stay entirely in memory
	config.MAX_SEGMENT_SIZE = 100 * 1024 * 1024 // 100 MB
	config.MAX_TOPIC_SIZE = 1000 * 1024 * 1024  // 1 GB
}

func BenchmarkCreateTopic(b *testing.B) {
	Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CreateTopic("topic-" + strconv.Itoa(i))
	}
}

func BenchmarkAppendToLog(b *testing.B) {
	Reset()
	topic := "bench-topic-append"
	CreateTopic(topic)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AppendToLog(topic, "test message")
	}
}

func BenchmarkAppendToLogParallel(b *testing.B) {
	Reset()
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
	Reset()
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
	Reset()
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
	Reset()
	for i := 0; i < 1000; i++ {
		CreateTopic(fmt.Sprintf("topic-get-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetTopics()
	}
}

// Capacity benchmarks focus on scaling sizes

// BenchmarkCapacity_1kTopics demonstrates creating a large number of topics.
func BenchmarkCapacity_1kTopics(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Reset()
		b.StopTimer()
		prefix := fmt.Sprintf("cap1k-%d-", i)
		b.StartTimer()

		for j := 0; j < 1000; j++ {
			CreateTopic(prefix + strconv.Itoa(j))
		}
	}
}

// BenchmarkCapacity_10kMessages tests publishing a large number of messages to a single topic
func BenchmarkCapacity_10kMessages(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Reset()
		b.StopTimer()
		topic := "capacity-topic-10k-msgs"
		CreateTopic(topic)
		b.StartTimer()

		// publish 10k messages to a single topic
		for j := 0; j < 10000; j++ {
			AppendToLog(topic, "capacity test payload data")
		}
	}
}

// BenchmarkCapacity_1kConsumers tests creating many consumers on a single topic
func BenchmarkCapacity_1kConsumers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Reset()
		b.StopTimer()
		topic := "capacity-topic-1k-consumers"
		CreateTopic(topic)
		prefix := fmt.Sprintf("cons1k-%d-", i)
		b.StartTimer()

		for j := 0; j < 1000; j++ {
			CreateConsumer(prefix+strconv.Itoa(j), topic)
		}
	}
}

// BenchmarkCapacity_MixedWorkload tests a realistic mixed workload
func BenchmarkCapacity_MixedWorkload(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Reset()
		b.StopTimer()
		prefix := fmt.Sprintf("mix-%d-", i)
		b.StartTimer()

		// System creates 100 topics and inserts 10 messages into each
		for j := 0; j < 100; j++ {
			topic := prefix + strconv.Itoa(j)
			CreateTopic(topic)
			for k := 0; k < 10; k++ {
				AppendToLog(topic, "mixed workload message")
			}
		}
	}
}

// BenchmarkCapacity_FullMix tests a fully mixed workload
func BenchmarkCapacity_FullMix(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Reset()
		b.StopTimer()
		prefix := fmt.Sprintf("full-%d-", i)
		b.StartTimer()

		// 10 topics, 10 consumers per topic, 10 msgs published & consumed per consumer
		for j := 0; j < 10; j++ {
			topic := prefix + strconv.Itoa(j)
			CreateTopic(topic)

			// Create 10 consumers per topic
			var consumers []string
			for c := 0; c < 10; c++ {
				consumer := "consumer-" + strconv.Itoa(c)
				CreateConsumer(consumer, topic)
				consumers = append(consumers, consumer)
			}

			// Publish 10 messages
			for k := 0; k < 10; k++ {
				AppendToLog(topic, "full mixed workload message payload")
			}

			// Consume messages across all consumers
			for _, consumer := range consumers {
				for m := 0; m < 10; m++ {
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
	topicLogs.Lock()
	defer topicLogs.Unlock()
	offsets.Lock()
	defer offsets.Unlock()
	topicConsumers.Lock()
	defer topicConsumers.Unlock()

	topics.Items = make(map[string]int)
	consumers.Items = make(map[string]int)
	topicLogs.Logs = make(map[string]*types.TopicLog)
	offsets.Positions = make(map[string]int)
	topicConsumers.Subscribers = make(map[string]map[string]int)
}

func TestSegmentationAndSealing(t *testing.T) {
	Reset()

	// 1. Back up original config values
	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDataDir := config.DATA_DIR

	// Clean up snapshots/data
	os.RemoveAll("./test_data")
	config.DATA_DIR = "./test_data"

	// 2. Set segment size and topic limits.
	config.MAX_SEGMENT_SIZE = 40
	config.MAX_TOPIC_SIZE = 100 // RAM eviction threshold at MAX_TOPIC_SIZE/2 = 50

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDataDir
		os.RemoveAll("./test_data")
	}()

	topic := "segment-topic"
	consumer := "segment-consumer"

	if err := CreateTopic(topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := CreateConsumer(consumer, topic); err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	// 3. Append messages to trigger segmentation.
	if err := AppendToLog(topic, "msg-00001"); err != nil {
		t.Fatalf("failed to append msg1: %v", err)
	}
	if err := AppendToLog(topic, "msg-00002"); err != nil {
		t.Fatalf("failed to append msg2: %v", err)
	}

	// Lock topic log to inspect
	topicLogs.RLock()
	tl, exists := topicLogs.Logs[topic]
	topicLogs.RUnlock()
	if !exists {
		t.Fatalf("topic log not found")
	}

	tl.RLock()
	segCount := len(tl.Segments)
	tl.RUnlock()
	if segCount != 1 {
		t.Fatalf("expected 1 segment, got %d", segCount)
	}

	if err := AppendToLog(topic, "msg-00003"); err != nil {
		t.Fatalf("failed to append msg3: %v", err)
	}

	tl.RLock()
	segCount = len(tl.Segments)
	if segCount != 2 {
		tl.RUnlock()
		t.Fatalf("expected 2 segments after rotation, got %d", segCount)
	}

	seg0 := tl.Segments[0]
	seg1 := tl.Segments[1]
	tl.RUnlock()

	seg0.RLock()
	if !seg0.Loaded {
		seg0.RUnlock()
		t.Errorf("expected Segment 0 to be loaded")
	}
	if seg0.Count != 3 {
		t.Errorf("expected Segment 0 count to be 3, got %d", seg0.Count)
	}
	seg0.RUnlock()

	seg1.RLock()
	if seg1.Count != 0 {
		t.Errorf("expected Segment 1 count to be 0, got %d", seg1.Count)
	}
	seg1.RUnlock()

	// 4. Append more messages to trigger RAM eviction of Segment 0.
	if err := AppendToLog(topic, "msg-00004"); err != nil {
		t.Fatalf("failed to append msg4: %v", err)
	}
	if err := AppendToLog(topic, "msg-00005"); err != nil {
		t.Fatalf("failed to append msg5: %v", err)
	}
	if err := AppendToLog(topic, "msg-00006"); err != nil {
		t.Fatalf("failed to append msg6: %v", err)
	}

	tl.RLock()
	segCount = len(tl.Segments)
	if segCount != 3 {
		tl.RUnlock()
		t.Fatalf("expected 3 segments, got %d", segCount)
	}
	tl.RUnlock()

	seg0.RLock()
	seg0Loaded := seg0.Loaded
	seg0Msgs := seg0.Messages
	seg0.RUnlock()

	if seg0Loaded || seg0Msgs != nil {
		t.Errorf("expected Segment 0 to be evicted from RAM")
	}

	// 5. Consume from the evicted segment. It should transparently reload from disk.
	config.MAX_TOPIC_SIZE = 300 // increase to prevent immediate re-eviction on reload
	msg, err := GetMessageFromLog(consumer, topic)
	if err != nil {
		t.Fatalf("failed to get message from evicted segment: %v", err)
	}
	if msg != "msg-00001" {
		t.Errorf("expected 'msg-00001', got %q", msg)
	}

	seg0.RLock()
	seg0LoadedAfter := seg0.Loaded
	seg0MsgsAfterCount := len(seg0.Messages)
	seg0.RUnlock()

	if !seg0LoadedAfter || seg0MsgsAfterCount == 0 {
		t.Errorf("expected Segment 0 to be reloaded after read")
	}
}

func TestRetentionDeletion(t *testing.T) {
	Reset()

	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDataDir := config.DATA_DIR

	os.RemoveAll("./test_retention_data")
	config.DATA_DIR = "./test_retention_data"
	config.MAX_SEGMENT_SIZE = 40
	config.MAX_TOPIC_SIZE = 60

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDataDir
		os.RemoveAll("./test_retention_data")
	}()

	topic := "retention-topic"
	consumer := "retention-consumer"

	if err := CreateTopic(topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := CreateConsumer(consumer, topic); err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	_ = AppendToLog(topic, "msg-00001")
	_ = AppendToLog(topic, "msg-00002")
	_ = AppendToLog(topic, "msg-00003") // seals Segment 0 (final size = 48)

	// Append Msg 4 -> written to Segment 1 (size = 16). Total topic size = 48 + 16 = 64 > 60.
	// This triggers deletion of Segment 0.
	if err := AppendToLog(topic, "msg-00004"); err != nil {
		t.Fatalf("failed to append Msg 4: %v", err)
	}

	topicLogs.RLock()
	tl, exists := topicLogs.Logs[topic]
	topicLogs.RUnlock()
	if !exists {
		t.Fatalf("topic log not found")
	}

	tl.RLock()
	segCount := len(tl.Segments)
	oldestSegID := tl.Segments[0].Id
	tl.RUnlock()

	if segCount != 1 {
		t.Errorf("expected 1 segment remaining (active), got %d", segCount)
	}
	if oldestSegID != 1 {
		t.Errorf("expected oldest segment to be ID 1 (Segment 0 deleted), got %d", oldestSegID)
	}

	seg0Path := filepath.Join(config.DATA_DIR, "topics", topic, "seg-0.log")
	if _, err := os.Stat(seg0Path); !os.IsNotExist(err) {
		t.Errorf("expected Segment 0 log file to be deleted, but it still exists")
	}

	// Consumer starts with offset 0. Segment 0 is deleted.
	// Offset 0 < Segment 1's BaseOffset (3), so it auto-resets to the active segment's BaseOffset (3).
	// This will consume "msg-00004".
	msg, err := GetMessageFromLog(consumer, topic)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}
	if msg != "msg-00004" {
		t.Errorf("expected msg-00004, got %q", msg)
	}

	off, err := GetOffset(consumer, topic)
	if err != nil || off != 4 {
		t.Errorf("expected offset 4, got %d with err %v", off, err)
	}
}

func TestLargeSegmentation(t *testing.T) {
	Reset()

	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDataDir := config.DATA_DIR

	os.RemoveAll("./test_large_data")
	config.DATA_DIR = "./test_large_data"
	config.MAX_SEGMENT_SIZE = 100
	config.MAX_TOPIC_SIZE = 20000

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDataDir
		os.RemoveAll("./test_large_data")
	}()

	topic := "large-segment-topic"
	consumer := "large-segment-consumer"

	if err := CreateTopic(topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := CreateConsumer(consumer, topic); err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	numMessages := 1000
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("msg-%05d", i)
		if err := AppendToLog(topic, msg); err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
	}

	topicLogs.RLock()
	tl, exists := topicLogs.Logs[topic]
	topicLogs.RUnlock()
	if !exists {
		t.Fatalf("topic log not found")
	}

	tl.RLock()
	segCount := len(tl.Segments)
	// Let's verify that a significant number of segments was created
	if segCount < 50 {
		tl.RUnlock()
		t.Fatalf("expected at least 50 segments, got %d", segCount)
	}

	// Verify that old segments are evicted from RAM
	evictedCount := 0
	for i := 0; i < segCount-1; i++ {
		seg := tl.Segments[i]
		seg.RLock()
		if !seg.Loaded {
			evictedCount++
		}
		seg.RUnlock()
	}
	tl.RUnlock()

	if evictedCount == 0 {
		t.Errorf("expected at least some segments to be evicted from RAM, got 0")
	}

	// Now consume all of them and verify ordering and transparent reloading
	for i := 0; i < numMessages; i++ {
		expectedMsg := fmt.Sprintf("msg-%05d", i)
		msg, err := GetMessageFromLog(consumer, topic)
		if err != nil {
			t.Fatalf("failed to consume message %d: %v", i, err)
		}
		if msg != expectedMsg {
			t.Errorf("expected %q at index %d, got %q", expectedMsg, i, msg)
		}
	}
}

func BenchmarkStorage_InMemory(b *testing.B) {
	// Configure large sizes so it remains entirely in memory
	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDir := config.DATA_DIR

	config.MAX_SEGMENT_SIZE = 100 * 1024 * 1024
	config.MAX_TOPIC_SIZE = 1000 * 1024 * 1024
	config.DATA_DIR = "./test_bench_inmem"

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDir
		os.RemoveAll("./test_bench_inmem")
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		Reset()
		os.RemoveAll("./test_bench_inmem")
		topic := "bench-inmem"
		consumer := "bench-inmem-consumer"
		CreateTopic(topic)
		CreateConsumer(consumer, topic)
		b.StartTimer()

		// Produce 200 messages
		for j := 0; j < 200; j++ {
			AppendToLog(topic, "msg-payload-data-for-inmem-bench-size-32")
		}

		// Consume 200 messages
		for j := 0; j < 200; j++ {
			_, err := GetMessageFromLog(consumer, topic)
			if err != nil {
				b.Fatalf("failed to consume: %v", err)
			}
		}
	}
}

func BenchmarkStorage_DiskSwap(b *testing.B) {
	// Configure small sizes to force frequent disk writes, evictions, and reloads
	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDir := config.DATA_DIR

	config.MAX_SEGMENT_SIZE = 40
	config.MAX_TOPIC_SIZE = 4000
	config.DATA_DIR = "./test_bench_disk"

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDir
		os.RemoveAll("./test_bench_disk")
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		Reset()
		os.RemoveAll("./test_bench_disk")
		topic := "bench-disk"
		consumer := "bench-disk-consumer"
		CreateTopic(topic)
		CreateConsumer(consumer, topic)
		b.StartTimer()

		// Produce 200 messages (triggers frequent segment sealing & disk writes)
		for j := 0; j < 200; j++ {
			AppendToLog(topic, "msg-payload-data-for-disk-bench-size-32")
		}

		// Consume 200 messages (triggers frequent disk reads & page loads)
		for j := 0; j < 200; j++ {
			_, err := GetMessageFromLog(consumer, topic)
			if err != nil {
				b.Fatalf("failed to consume: %v", err)
			}
		}
	}
}

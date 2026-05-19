package memstore

import (
	"fmt"
	"math/rand"
	"os"
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

	// 2. Set very small segment size limit and topic size limit.
	// unsafe.Sizeof(string) is typically 16 bytes.
	// We want to force rotation on 3rd message.
	// Two messages of 16 bytes each is 32. Let's set MAX_SEGMENT_SIZE to 40.
	// And set MAX_TOPIC_SIZE to 60, so that once we have two sealed segments and
	// write another, we evict the oldest segment.
	config.MAX_SEGMENT_SIZE = 40
	config.MAX_TOPIC_SIZE = 60

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
	// Message size is 16 bytes.
	// Msg 1: Size = 16. Segment 0 (active).
	// Msg 2: Size = 32. Segment 0 (active).
	// Msg 3: Size = 48. This is >= MAX_SEGMENT_SIZE (40), so:
	//   - Segment 0 is sealed (written to disk and kept in memory)
	//   - Segment 1 (active) is created with Msg 3.
	if err := AppendToLog(topic, "msg-00001"); err != nil { // 16 bytes
		t.Fatalf("failed to append msg1: %v", err)
	}
	if err := AppendToLog(topic, "msg-00002"); err != nil { // 16 bytes
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

	// Append msg-00003. This exceeds MAX_SEGMENT_SIZE (40) since the existing segment size was 32,
	// and msg-00003 adds 16. Total size before rotation checks would be 48 >= 40.
	// It should trigger sealing of Segment 0 and rotation to Segment 1.
	if err := AppendToLog(topic, "msg-00003"); err != nil {
		t.Fatalf("failed to append msg3: %v", err)
	}

	tl.RLock()
	segCount = len(tl.Segments)
	if segCount != 2 {
		tl.RUnlock()
		t.Fatalf("expected 2 segments after rotation, got %d", segCount)
	}

	// Segment 0 must be sealed, written to disk, and still loaded.
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

	// 4. Append more messages to trigger eviction of Segment 0.
	// Since MAX_TOPIC_SIZE = 60, and loaded sizes are:
	// Seg 0: 48 bytes
	// Seg 1: 0 bytes
	// Total loaded size is 48.
	// Let's append Msg 4 (16 bytes) into Seg 1. Seg 1 size is 16. Total loaded: 64. No rotation.
	if err := AppendToLog(topic, "msg-00004"); err != nil {
		t.Fatalf("failed to append msg4: %v", err)
	}
	// Let's append Msg 5 (16 bytes) into Seg 1. Seg 1 size is 32. Total loaded: 80. No rotation.
	if err := AppendToLog(topic, "msg-00005"); err != nil {
		t.Fatalf("failed to append msg5: %v", err)
	}
	// Let's append Msg 6 (16 bytes) into Seg 1. Seg 1 size is 48. This triggers rotation.
	// Total loaded before rotation check: Seg 0 (48) + Seg 1 (48) + Seg 2 (0) = 96 > MAX_TOPIC_SIZE (60).
	// This triggers eviction of Segment 0 (oldest sealed segment).
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
	config.MAX_TOPIC_SIZE = 100
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

func TestLargeSegmentation(t *testing.T) {
	Reset()

	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDataDir := config.DATA_DIR

	os.RemoveAll("./test_large_data")
	config.DATA_DIR = "./test_large_data"
	config.MAX_SEGMENT_SIZE = 100
	config.MAX_TOPIC_SIZE = 300

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

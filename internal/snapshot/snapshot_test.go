package snapshot

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/chahatsagarmain/GoStream/config"
	"github.com/chahatsagarmain/GoStream/internal/memstore"
)

func TestSnapshotSaveAndRestore(t *testing.T) {
	// 1. Clear out memstore
	memstore.RestoreStore(nil, nil, nil, nil)

	// 2. Clear out any previous snapshots
	os.RemoveAll("./snapshots")

	// 3. Populate memory store
	topic := "test-snapshot-topic"
	consumer := "test-snapshot-consumer"
	memstore.CreateTopic(topic)
	memstore.CreateConsumer(consumer, topic)
	memstore.AppendToLog(topic, "msg1")
	memstore.AppendToLog(topic, "msg2")
	// Note: offset must be tested
	memstore.SetOffset(consumer, topic, 1) // Next read should be msg2

	// 4. Create Snapshot instance and push to disc
	snap := NewSnapShot()
	snap.takeSnapShot()

	err := snap.saveSnapShot()
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// 5. Clear out memstore to simulate crash
	memstore.RestoreStore(nil, nil, nil, nil)
	if len(memstore.GetTopics()) != 0 {
		t.Fatalf("Memstore was not cleared properly")
	}

	// 6. Restore from snapshot
	err = RestoreSnapShot()
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	// 7. Verify all data matches what was seeded
	topics := memstore.GetTopics()
	if len(topics) != 1 || topics[0] != topic {
		t.Errorf("Expected topic %q, got %v", topic, topics)
	}

	consumers := memstore.GetConsumers()
	if len(consumers) != 1 || consumers[0] != consumer {
		t.Errorf("Expected consumer %q, got %v", consumer, consumers)
	}

	offset, err := memstore.GetOffset(consumer, topic)
	if err != nil || offset != 1 {
		t.Errorf("Expected offset 1, got %v, err: %v", offset, err)
	}

	// Verify messages via consuming
	msg, err := memstore.GetMessageFromLog(consumer, topic)
	if err != nil || msg != "msg2" {
		t.Errorf("Expected message 'msg2', got %q, err: %v", msg, err)
	}

	// Clean up snapshots folder
	os.RemoveAll("./snapshots")
}

func TestNoSnapshotsFound(t *testing.T) {
	os.RemoveAll("./snapshots")

	err := RestoreSnapShot()
	if err != nil {
		t.Fatalf("RestoreSnapShot on empty directory should not error, got %v", err)
	}
}

func TestLargeSnapshotRestore(t *testing.T) {
	// 1. Clear out memstore & snapshots
	memstore.RestoreStore(nil, nil, nil, nil)
	os.RemoveAll("./snapshots")
	os.RemoveAll("./test_large_snapshot_data")

	// 2. Configure smaller constraints
	origMaxSegmentSize := config.MAX_SEGMENT_SIZE
	origMaxTopicSize := config.MAX_TOPIC_SIZE
	origDataDir := config.DATA_DIR

	config.MAX_SEGMENT_SIZE = 100
	config.MAX_TOPIC_SIZE = 10000
	config.DATA_DIR = "./test_large_snapshot_data"

	defer func() {
		config.MAX_SEGMENT_SIZE = origMaxSegmentSize
		config.MAX_TOPIC_SIZE = origMaxTopicSize
		config.DATA_DIR = origDataDir
		os.RemoveAll("./snapshots")
		os.RemoveAll("./test_large_snapshot_data")
	}()

	topic := "large-snap-topic"
	consumer := "large-snap-consumer"

	if err := memstore.CreateTopic(topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := memstore.CreateConsumer(consumer, topic); err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	numMessages := 500
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("msg-%05d", i)
		if err := memstore.AppendToLog(topic, msg); err != nil {
			t.Fatalf("failed to append msg %d: %v", i, err)
		}
	}

	// Move consumer offset forward by reading some messages
	readCount := 100
	for i := 0; i < readCount; i++ {
		expectedMsg := fmt.Sprintf("msg-%05d", i)
		msg, err := memstore.GetMessageFromLog(consumer, topic)
		if err != nil {
			t.Fatalf("failed to consume message %d: %v", i, err)
		}
		if msg != expectedMsg {
			t.Errorf("expected %q, got %q", expectedMsg, msg)
		}
	}

	// 3. Take snapshot
	snap := NewSnapShot()
	snap.takeSnapShot()
	if err := snap.saveSnapShot(); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// 4. Reset memstore (simulating crash)
	memstore.RestoreStore(nil, nil, nil, nil)
	if len(memstore.GetTopics()) != 0 {
		t.Fatalf("memstore was not cleared properly")
	}

	// 5. Restore from snapshot
	if err := RestoreSnapShot(); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	// 6. Verify consumer starts from the correct offset (index 100) and reads the rest of the messages
	for i := readCount; i < numMessages; i++ {
		expectedMsg := fmt.Sprintf("msg-%05d", i)
		msg, err := memstore.GetMessageFromLog(consumer, topic)
		if err != nil {
			t.Fatalf("failed to consume message after restore %d: %v", i, err)
		}
		if msg != expectedMsg {
			t.Errorf("expected %q after restore, got %q", expectedMsg, msg)
		}
	}
}

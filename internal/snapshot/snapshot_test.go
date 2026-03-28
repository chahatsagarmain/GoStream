package snapshot

import (
	"os"
	"testing"
	"time"

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

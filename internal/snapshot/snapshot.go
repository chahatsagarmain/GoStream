package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chahatsagarmain/GoStream/internal/memstore"
)

// SnapShot holds the exported fields so json.Marshal can serialize them.
// Without exported fields (Capitalized names), json won't capture them.
type SnapShot struct {
	Topics    []string            `json:"topics"`
	Consumers []string            `json:"consumers"`
	Messages  map[string][]string `json:"messages"`
	Offsets   map[string]int      `json:"offsets"`
}

func NewSnapShot() *SnapShot {
	return &SnapShot{
		Topics:    make([]string, 0),
		Consumers: make([]string, 0),
		Messages:  make(map[string][]string),
		Offsets:   make(map[string]int),
	}
}

func (s *SnapShot) StartSnapShot() {
	go func() {
		for {
			s.takeSnapShot()
			if err := s.saveSnapShot(); err != nil {
				fmt.Printf("Failed to save snapshot: %v\n", err)
			}
			time.Sleep(10 * time.Second)
		}
	}()
}

func (s *SnapShot) takeSnapShot() {
	s.Topics = memstore.GetTopics()
	s.Consumers = memstore.GetConsumers()
	s.Offsets = memstore.GetAllOffsets()
	s.Messages = memstore.GetAllMessages()
}

func (s *SnapShot) saveSnapShot() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	dir := "./snapshots"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Used Unix epoch since time.Now().String() generates colons which are illegal filename characters in windows!
	snapShotName := fmt.Sprintf("snapshot_%d.json", time.Now().Unix())
	path := filepath.Join(dir, snapShotName)

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}

// RestoreSnapShot finds the latest snapshot in ./snapshots and loads it into memstore
func RestoreSnapShot() error {
	dir := "./snapshots"
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no snapshots yet, so boot empty
		}
		return err
	}

	var latestFile string
	var latestTime time.Time

	// Find the most recently modified snapshot chunk
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") && strings.HasPrefix(info.Name(), "snapshot_") {
			if info.ModTime().After(latestTime) {
				latestTime = info.ModTime()
				latestFile = info.Name()
			}
		}
	}

	if latestFile == "" {
		return nil // no valid snapshot found
	}

	path := filepath.Join(dir, latestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var snap SnapShot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	memstore.RestoreStore(snap.Topics, snap.Consumers, snap.Messages, snap.Offsets)
	fmt.Printf("Successfully restored store state from %s\n", latestFile)
	return nil
}

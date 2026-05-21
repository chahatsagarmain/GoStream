package types

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Topics struct {
	sync.RWMutex
	Items map[string]int // O(1) access
}

type Consumers struct {
	sync.RWMutex
	Items map[string]int // O(1) access
}

type TopicLog struct {
	sync.RWMutex
	Segments []*Segment
	Size     int
}

type TopicLogs struct {
	sync.RWMutex
	Logs map[string]*TopicLog
}

type Segment struct {
	sync.RWMutex
	Id         int
	Messages   []string
	Count      int
	Size       int
	BaseOffset int
	Loaded     bool
}

type Offsets struct {
	sync.RWMutex
	Positions map[string]int // topicname:consumername -> offset integer mapping here
}

type TopicConsumers struct {
	sync.RWMutex
	Subscribers map[string]map[string]int // topic -> set{consumerID}
}

// WriteToDisk persists the segment's messages to path as JSOn and guarantees durability using fsync.
func (s *Segment) WriteToDisk(path string) error {
	s.RLock()
	defer s.RUnlock()

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, msg := range s.Messages {
		line, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// fsync to flush all OS writes to physical disk media
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

// AppendMessageToDisk appends a single message to the segment file on disk.
func (s *Segment) AppendMessageToDisk(path, message string) error {
	s.Lock()
	defer s.Unlock()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	line, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		return err
	}
	if _, err := f.Write([]byte("\n")); err != nil {
		return err
	}
	return f.Sync()
}

// LoadFromDisk loads messages from path as JSON back into RAM.
func (s *Segment) LoadFromDisk(path string) error {
	s.Lock()
	defer s.Unlock()
	if s.Loaded {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s.Messages = make([]string, 0, s.Count)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var msg string
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			return err
		}
		s.Messages = append(s.Messages, msg)
	}
	s.Loaded = true
	return scanner.Err()
}

// free memory.
func (s *Segment) Evict() {
	s.Lock()
	defer s.Unlock()
	if !s.Loaded {
		return
	}
	s.Messages = nil
	s.Loaded = false
}

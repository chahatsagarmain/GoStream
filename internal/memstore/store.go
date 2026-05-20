package memstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/chahatsagarmain/GoStream/config"
	"github.com/chahatsagarmain/GoStream/internal/memstore/types"
)

var (
	// topics is a list of all topics
	topics = types.Topics{
		Items: make(map[string]int),
	}

	consumers = types.Consumers{
		Items: make(map[string]int),
	}

	topicLogs = types.TopicLogs{
		Logs: make(map[string]*types.TopicLog),
	}

	offsets = types.Offsets{
		Positions: make(map[string]int),
	}

	topicConsumers = types.TopicConsumers{
		Subscribers: make(map[string]map[string]int),
	}
)

// CreateTopic adds a topic if it doesn't exist
func CreateTopic(topicname string) error {
	topics.Lock()
	defer topics.Unlock()

	contains := strings.Contains(topicname, ":")
	if contains {
		return fmt.Errorf("wrong topic name format")
	}
	// Check if topic exists
	if _, exists := topics.Items[topicname]; exists {
		return nil
	}

	topics.Items[topicname] = 1

	topicLogs.Lock()
	topicLogs.Logs[topicname] = &types.TopicLog{
		Segments: []*types.Segment{
			{
				Id:         0,
				BaseOffset: 0,
				Loaded:     true,
				Messages:   make([]string, 0),
			},
		},
	}
	topicLogs.Unlock()

	topicConsumers.Lock()
	if _, ok := topicConsumers.Subscribers[topicname]; !ok {
		topicConsumers.Subscribers[topicname] = make(map[string]int)
	}
	topicConsumers.Unlock()

	return nil
}

// GetTopics returns all topics
func GetTopics() []string {
	topics.RLock()
	defer topics.RUnlock()

	result := make([]string, 0, len(topics.Items))
	for key := range topics.Items {
		result = append(result, key)
	}

	return result
}

// DeleteTopic removes a topic and its messages
func DeleteTopic(topicname string) error {
	topics.Lock()
	defer topics.Unlock()

	delete(topics.Items, topicname)

	topicLogs.Lock()
	delete(topicLogs.Logs, topicname)
	topicLogs.Unlock()

	offsets.Lock()
	for key := range offsets.Positions {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 && parts[0] == topicname {
			delete(offsets.Positions, key)
		}
	}
	offsets.Unlock()

	topicConsumers.Lock()
	delete(topicConsumers.Subscribers, topicname)
	topicConsumers.Unlock()

	// Delete segment files from disk for the topicsname
	dir := filepath.Join(config.DATA_DIR, "topics", topicname)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}

	return nil
}

// CreateConsumer adds a consumer and initializes its offset
func CreateConsumer(consumer, topicname string) error {
	topics.RLock()
	topicExists := false

	_, topicExists = topics.Items[topicname]

	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topicname)
	}

	consumers.Lock()
	exists := false
	_, exists = consumers.Items[consumer]

	if !exists {
		consumers.Items[consumer] = 1
	}
	consumers.Unlock()

	offsets.Lock()
	key := fmt.Sprintf("%s:%s", topicname, consumer)
	offsets.Positions[key] = 0
	offsets.Unlock()

	topicConsumers.Lock()
	if _, ok := topicConsumers.Subscribers[topicname]; !ok {
		topicConsumers.Subscribers[topicname] = make(map[string]int)
	}
	topicConsumers.Subscribers[topicname][consumer] = 1
	topicConsumers.Unlock()

	return nil
}

// GetConsumers returns all consumers
func GetConsumers() []string {
	consumers.RLock()
	defer consumers.RUnlock()

	result := make([]string, 0, len(consumers.Items))
	for key := range consumers.Items {
		result = append(result, key)
	}
	return result
}

// DeleteConsumer removes a consumer and its offsets
func DeleteConsumer(consumername string) error {
	consumers.Lock()
	defer consumers.Unlock()

	if _, found := consumers.Items[consumername]; !found {
		return nil
	}
	delete(consumers.Items, consumername)

	offsets.Lock()
	for key := range offsets.Positions {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 && parts[1] == consumername {
			delete(offsets.Positions, key)
		}
	}
	offsets.Unlock()

	// Remove from topicConsumers index
	topicConsumers.Lock()
	for _, subs := range topicConsumers.Subscribers {
		delete(subs, consumername)
	}
	topicConsumers.Unlock()

	return nil
}

// GetConsumersByTopic returns all consumer IDs subscribed to the given topic
func GetConsumersByTopic(topicname string) ([]string, error) {
	topics.RLock()
	_, exists := topics.Items[topicname]
	topics.RUnlock()
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicname)
	}

	topicConsumers.RLock()
	defer topicConsumers.RUnlock()
	subs := topicConsumers.Subscribers[topicname]
	result := make([]string, 0, len(subs))
	for c := range subs {
		result = append(result, c)
	}
	return result, nil
}

// SetOffset updates a consumer's position in a topic
func SetOffset(consumer, topic string, value int) error {
	topics.RLock()
	topicExists := false
	_, topicExists = topics.Items[topic]
	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	key := fmt.Sprintf("%s:%s", topic, consumer)
	offsets.Lock()
	offsets.Positions[key] = value
	offsets.Unlock()

	return nil
}

// GetOffset retrieves a consumer's position in a topic
func GetOffset(consumer, topic string) (int, error) {
	topics.RLock()
	topicExists := false
	_, topicExists = topics.Items[topic]
	topics.RUnlock()

	if !topicExists {
		return -1, fmt.Errorf("topic %s does not exist", topic)
	}

	key := fmt.Sprintf("%s:%s", topic, consumer)
	offsets.RLock()
	offset, exists := offsets.Positions[key]
	offsets.RUnlock()

	if !exists {
		return -1, fmt.Errorf("no offset found for consumer %s on topic %s", consumer, topic)
	}

	return offset, nil
}

// AppendToLog adds a message to a topic's message log
func AppendToLog(topic, message string) error {
	topics.RLock()
	_, topicExists := topics.Items[topic]
	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	topicLogs.Lock()
	tl, exists := topicLogs.Logs[topic]
	if !exists {
		tl = &types.TopicLog{
			Segments: []*types.Segment{
				{
					Id:         0,
					BaseOffset: 0,
					Loaded:     true,
					Messages:   make([]string, 0),
				},
			},
		}
		topicLogs.Logs[topic] = tl
	}
	topicLogs.Unlock()

	tl.Lock()
	defer tl.Unlock()

	// Get active segment which should be the last segment
	active := tl.Segments[len(tl.Segments)-1]

	active.Lock()
	active.Messages = append(active.Messages, message)
	active.Count++
	// Calculate new size: approximate size as length of message + 1 for newline
	msgSize := int(unsafe.Sizeof(message))
	active.Size += msgSize
	tl.Size += msgSize
	active.Unlock()

	// Check if active segment needs rotation to the disk
	if active.Size >= config.MAX_SEGMENT_SIZE {
		// Create segment directory if not exists
		dir := filepath.Join(config.DATA_DIR, "topics", topic)
		os.MkdirAll(dir, 0755)

		// Persist the current active segment
		path := filepath.Join(dir, fmt.Sprintf("seg-%d.log", active.Id))
		if err := active.WriteToDisk(path); err != nil {
			return fmt.Errorf("failed to persist active segment: %w", err)
		}

		newSeg := &types.Segment{
			Id:         active.Id + 1,
			BaseOffset: active.BaseOffset + active.Count,
			Loaded:     true,
			Messages:   make([]string, 0),
		}
		tl.Segments = append(tl.Segments, newSeg)

		// Evict older sealed segments if total topic size exceeds the max size for the topic

		evictSealedSegments(tl, topic)

	}

	return nil
}

// evictSealedSegments evicts sealed segments from RAM to keep memory usage bounded
func evictSealedSegments(tl *types.TopicLog, topic string) {
	// Calculate what is currently loaded in RAM
	loadedSize := 0
	for _, seg := range tl.Segments {
		seg.RLock()
		if seg.Loaded {
			loadedSize += seg.Size
		}
		seg.RUnlock()
	}

	// Evict from oldest to newest sealed segment (exclude the active segment)
	for i := 0; i < len(tl.Segments)-1; i++ {
		if loadedSize < config.MAX_TOPIC_SIZE {
			break
		}
		seg := tl.Segments[i]
		seg.Lock()
		if seg.Loaded {
			dir := filepath.Join(config.DATA_DIR, "topics", topic)
			os.MkdirAll(dir, 0755)
			path := filepath.Join(dir, fmt.Sprintf("seg-%d.log", seg.Id))
			seg.Unlock()
			_ = seg.WriteToDisk(path)

			seg.Lock()
			// clear the in-memory message
			if seg.Loaded {
				loadedSize -= seg.Size
				seg.Messages = nil
				seg.Loaded = false
			}
		}
		seg.Unlock()
	}
}

// GetMessageFromLog fetches a message from a topic's log at the consumer's current offset
func GetMessageFromLog(consumer, topic string) (string, error) {
	offset, err := GetOffset(consumer, topic)
	if err != nil {
		return "", err
	}

	topicLogs.RLock()
	tl, exists := topicLogs.Logs[topic]
	topicLogs.RUnlock()

	if !exists {
		return "", fmt.Errorf("no messages for topic %s", topic)
	}

	tl.Lock()
	defer tl.Unlock()

	var targetSeg *types.Segment
	for _, seg := range tl.Segments {
		seg.RLock()
		if offset >= seg.BaseOffset && offset < seg.BaseOffset+seg.Count {
			targetSeg = seg
			seg.RUnlock()
			break
		}
		seg.RUnlock()
	}

	// If offset is out of bounds or in the active segment
	if targetSeg == nil {
		active := tl.Segments[len(tl.Segments)-1]
		active.RLock()
		if offset >= active.BaseOffset && offset < active.BaseOffset+active.Count {
			targetSeg = active
		}
		active.RUnlock()
	}

	if targetSeg == nil {
		return "", fmt.Errorf("offset out of bounds")
	}

	// load the segment  to memory from disk
	targetSeg.Lock()
	if !targetSeg.Loaded {
		dir := filepath.Join(config.DATA_DIR, "topics", topic)
		path := filepath.Join(dir, fmt.Sprintf("seg-%d.log", targetSeg.Id))
		targetSeg.Unlock()
		if err := targetSeg.LoadFromDisk(path); err != nil {
			return "", fmt.Errorf("failed to load segment from disk: %w", err)
		}
		targetSeg.Lock()
	}

	// fetch message from base offset and baseoffset
	localOffset := offset - targetSeg.BaseOffset
	if localOffset < 0 || localOffset >= len(targetSeg.Messages) {
		targetSeg.Unlock()
		return "", fmt.Errorf("local offset out of bounds")
	}
	msg := targetSeg.Messages[localOffset]
	targetSeg.Unlock()

	// Evict older segments if loading if loading the segmeent pushed the max size limit
	evictSealedSegments(tl, topic)

	// Increment offset
	if err := SetOffset(consumer, topic, offset+1); err != nil {
		return "", fmt.Errorf("failed to update offset: %w", err)
	}

	return msg, nil
}

func GetAllOffsets() map[string]int {
	offsets.RLock()
	defer offsets.RUnlock()
	return offsets.Positions
}

func GetAllMessages() map[string][]string {
	topicLogs.RLock()
	defer topicLogs.RUnlock()

	res := make(map[string][]string)
	for name, tl := range topicLogs.Logs {
		tl.Lock()
		var msgs []string
		for _, seg := range tl.Segments {
			seg.Lock()
			if !seg.Loaded {
				dir := filepath.Join(config.DATA_DIR, "topics", name)
				path := filepath.Join(dir, fmt.Sprintf("seg-%d.log", seg.Id))
				seg.Unlock()
				_ = seg.LoadFromDisk(path)
				seg.Lock()
			}
			for _, val := range seg.Messages {
				msgs = append(msgs, val)
			}
			seg.Unlock()
		}
		res[name] = msgs
		tl.Unlock()
	}
	return res
}

// RestoreStore injects snapshot data by globally locking all core structures
func RestoreStore(newTopics []string, newConsumers []string, newMessages map[string][]string, newOffsets map[string]int) {
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

	if newTopics == nil {
		newTopics = make([]string, 0)
	}
	if newConsumers == nil {
		newConsumers = make([]string, 0)
	}
	if newMessages == nil {
		newMessages = make(map[string][]string)
	}
	if newOffsets == nil {
		newOffsets = make(map[string]int)
	}

	newTopicItems := make(map[string]int)
	for _, val := range newTopics {
		newTopicItems[val] = 1
	}

	newConsumerItems := make(map[string]int)
	for _, val := range newConsumers {
		newConsumerItems[val] = 1
	}

	// Restore segment logs
	newLogs := make(map[string]*types.TopicLog)
	for topic, msgs := range newMessages {
		tl := &types.TopicLog{
			Segments: make([]*types.Segment, 0),
		}

		size := 0
		currId := 0
		seg := &types.Segment{
			Id:         currId,
			BaseOffset: 0,
			Loaded:     true,
			Messages:   make([]string, 0),
			Count:      0,
			Size:       0,
		}
		baseOffset := 0
		for _, m := range msgs {
			msgSize := int(unsafe.Sizeof(m))
			if size+msgSize > config.MAX_SEGMENT_SIZE && seg.Count > 0 {
				tl.Segments = append(tl.Segments, seg)
				baseOffset += seg.Count
				currId++
				size = 0
				seg = &types.Segment{
					Id:         currId,
					BaseOffset: baseOffset,
					Loaded:     true,
					Messages:   make([]string, 0),
					Count:      0,
					Size:       0,
				}
			}
			seg.Messages = append(seg.Messages, m)
			seg.Count++
			seg.Size += msgSize
			size += msgSize
		}
		tl.Segments = append(tl.Segments, seg)

		tlsize := 0
		for _, s := range tl.Segments {
			tlsize += s.Size
		}
		tl.Size = tlsize
		evictSealedSegments(tl, topic)
		newLogs[topic] = tl
	}

	newSubscribers := make(map[string]map[string]int)
	for _, t := range newTopics {
		newSubscribers[t] = make(map[string]int)
	}
	for key := range newOffsets {
		for _, t := range newTopics {
			prefix := t + ":"
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				consumerID := key[len(prefix):]
				newSubscribers[t][consumerID] = 1
			}
		}
	}

	topics.Items = newTopicItems
	consumers.Items = newConsumerItems
	topicLogs.Logs = newLogs
	offsets.Positions = newOffsets
	topicConsumers.Subscribers = newSubscribers
}

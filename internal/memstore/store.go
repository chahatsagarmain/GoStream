package memstore

import (
	"fmt"
	"sync"
)

var (
	// topics is a list of all topics
	topics = struct {
		sync.RWMutex
		items []string
	}{
		items: make([]string, 0),
	}

	// consumers is a list of all consumers
	consumers = struct {
		sync.RWMutex
		items []string
	}{
		items: make([]string, 0),
	}

	// messages stores topic messages as append-only logs
	messages = struct {
		sync.RWMutex
		logs map[string][]string // topicname -> []messages
	}{
		logs: make(map[string][]string),
	}

	// offsets stores consumer positions in topics
	offsets = struct {
		sync.RWMutex
		positions map[string]int // "topic:consumer" -> offset
	}{
		positions: make(map[string]int),
	}
)

// CreateTopic adds a topic if it doesn't exist
func CreateTopic(topicname string) error {
	topics.Lock()
	defer topics.Unlock()

	// Check if topic exists
	for _, t := range topics.items {
		if t == topicname {
			return nil // already exists
		}
	}

	topics.items = append(topics.items, topicname)

	// Initialize message log for topic
	messages.Lock()
	messages.logs[topicname] = make([]string, 0)
	messages.Unlock()

	return nil
}

// GetTopics returns all topics
func GetTopics() []string {
	topics.RLock()
	defer topics.RUnlock()

	result := make([]string, len(topics.items))
	copy(result, topics.items)
	return result
}

// DeleteTopic removes a topic and its messages
func DeleteTopic(topicname string) error {
	topics.Lock()
	defer topics.Unlock()

	found := false
	for i, t := range topics.items {
		if t == topicname {
			// Remove from topics list
			topics.items = append(topics.items[:i], topics.items[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return nil
	}

	// Remove messages
	messages.Lock()
	delete(messages.logs, topicname)
	messages.Unlock()

	// Remove related offsets
	offsets.Lock()
	for key := range offsets.positions {
		if key[:len(topicname)] == topicname {
			delete(offsets.positions, key)
		}
	}
	offsets.Unlock()

	return nil
}

// CreateConsumer adds a consumer and initializes its offset
func CreateConsumer(consumer, topicname string) error {
	// Check if topic exists
	topics.RLock()
	topicExists := false
	for _, t := range topics.items {
		if t == topicname {
			topicExists = true
			break
		}
	}
	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topicname)
	}

	consumers.Lock()
	// Check if consumer exists
	exists := false
	for _, c := range consumers.items {
		if c == consumer {
			exists = true
			break
		}
	}

	if !exists {
		consumers.items = append(consumers.items, consumer)
	}
	consumers.Unlock()

	// Initialize offset
	offsets.Lock()
	key := fmt.Sprintf("%s:%s", topicname, consumer)
	offsets.positions[key] = 0
	offsets.Unlock()

	return nil
}

// GetConsumers returns all consumers
func GetConsumers() []string {
	consumers.RLock()
	defer consumers.RUnlock()

	result := make([]string, len(consumers.items))
	copy(result, consumers.items)
	return result
}

// DeleteConsumer removes a consumer and its offsets
func DeleteConsumer(consumername string) error {
	consumers.Lock()
	defer consumers.Unlock()

	found := false
	for i, c := range consumers.items {
		if c == consumername {
			consumers.items = append(consumers.items[:i], consumers.items[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return nil
	}

	// Remove consumer offsets
	offsets.Lock()
	for key := range offsets.positions {
		if key[len(key)-len(consumername):] == consumername {
			delete(offsets.positions, key)
		}
	}
	offsets.Unlock()

	return nil
}

// SetOffset updates a consumer's position in a topic
func SetOffset(consumer, topic string, value int) error {
	// Check if topic exists
	topics.RLock()
	topicExists := false
	for _, t := range topics.items {
		if t == topic {
			topicExists = true
			break
		}
	}
	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	key := fmt.Sprintf("%s:%s", topic, consumer)
	offsets.Lock()
	offsets.positions[key] = value
	offsets.Unlock()

	return nil
}

// GetOffset retrieves a consumer's position in a topic
func GetOffset(consumer, topic string) (int, error) {
	// Check if topic exists
	topics.RLock()
	topicExists := false
	for _, t := range topics.items {
		if t == topic {
			topicExists = true
			break
		}
	}
	topics.RUnlock()

	if !topicExists {
		return -1, fmt.Errorf("topic %s does not exist", topic)
	}

	key := fmt.Sprintf("%s:%s", topic, consumer)
	offsets.RLock()
	offset, exists := offsets.positions[key]
	offsets.RUnlock()

	if !exists {
		return -1, fmt.Errorf("no offset found for consumer %s on topic %s", consumer, topic)
	}

	return offset, nil
}

// AppendToLog adds a message to a topic's message log
func AppendToLog(topic, message string) error {
	// Check if topic exists
	topics.RLock()
	topicExists := false
	for _, t := range topics.items {
		if t == topic {
			topicExists = true
			break
		}
	}
	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	messages.Lock()
	messages.logs[topic] = append(messages.logs[topic], message)
	messages.Unlock()

	return nil
}

// GetMessageFromLog fetches a message from a topic's log at the consumer's current offset
func GetMessageFromLog(consumer, topic string) (string, error) {
	offset, err := GetOffset(consumer, topic)
	if err != nil {
		return "", err
	}

	messages.RLock()
	log, exists := messages.logs[topic]
	if !exists {
		messages.RUnlock()
		return "", fmt.Errorf("no messages for topic %s", topic)
	}

	if offset >= len(log) {
		messages.RUnlock()
		return "", fmt.Errorf("offset out of bounds")
	}

	message := log[offset]
	messages.RUnlock()

	// Increment offset
	if err := SetOffset(consumer, topic, offset+1); err != nil {
		return "", fmt.Errorf("failed to update offset: %w", err)
	}

	return message, nil
}
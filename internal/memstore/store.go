package memstore

import (
	"fmt"
	"strings"
	"sync"
)

var (
	// topics is a list of all topics
	topics = struct {
		sync.RWMutex
		items map[string]int
	}{
		items: make(map[string]int),
	}

	consumers = struct {
		sync.RWMutex
		items map[string]int
	}{
		items: make(map[string]int),
	}

	messages = struct {
		sync.RWMutex
		logs map[string][]string // topicname -> []messages
	}{
		logs: make(map[string][]string),
	}

	offsets = struct {
		sync.RWMutex
		positions map[string]int // "topic:consumer" -> offset
	}{
		positions: make(map[string]int),
	}

	topicConsumers = struct {
		sync.RWMutex
		subscribers map[string]map[string]struct{} // topic -> set{consumerID}
	}{
		subscribers: make(map[string]map[string]struct{}),
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
	if _, exists := topics.items[topicname]; exists {
		return nil
	}

	topics.items[topicname] = 1

	messages.Lock()
	messages.logs[topicname] = make([]string, 0)
	messages.Unlock()

	topicConsumers.Lock()
	topicConsumers.subscribers[topicname] = make(map[string]struct{})
	topicConsumers.Unlock()

	return nil
}

// GetTopics returns all topics
func GetTopics() []string {
	topics.RLock()
	defer topics.RUnlock()

	result := make([]string, 0, len(topics.items))
	for key, _ := range topics.items {
		result = append(result, key)
	}

	return result
}

// DeleteTopic removes a topic and its messages
func DeleteTopic(topicname string) error {
	topics.Lock()
	defer topics.Unlock()

	delete(topics.items, topicname)

	messages.Lock()
	delete(messages.logs, topicname)
	messages.Unlock()

	offsets.Lock()
	for key := range offsets.positions {
		if key[:len(topicname)] == topicname {
			delete(offsets.positions, key)
		}
	}
	offsets.Unlock()

	topicConsumers.Lock()
	delete(topicConsumers.subscribers, topicname)
	topicConsumers.Unlock()

	return nil
}

// CreateConsumer adds a consumer and initializes its offset
func CreateConsumer(consumer, topicname string) error {
	// Check if topic exists
	topics.RLock()
	topicExists := false

	_, topicExists = topics.items[topicname]

	topics.RUnlock()

	if !topicExists {
		return fmt.Errorf("topic %s does not exist", topicname)
	}

	consumers.Lock()
	exists := false
	_, exists = consumers.items[consumer]

	if !exists {
		consumers.items[consumer] = 1
	}
	consumers.Unlock()

	offsets.Lock()
	key := fmt.Sprintf("%s:%s", topicname, consumer)
	offsets.positions[key] = 0
	offsets.Unlock()

	topicConsumers.Lock()
	if _, ok := topicConsumers.subscribers[topicname]; !ok {
		topicConsumers.subscribers[topicname] = make(map[string]struct{})
	}
	topicConsumers.subscribers[topicname][consumer] = struct{}{}
	topicConsumers.Unlock()

	return nil
}

// GetConsumers returns all consumers
func GetConsumers() []string {
	consumers.RLock()
	defer consumers.RUnlock()

	result := make([]string, 0, len(consumers.items))
	for key := range consumers.items {
		result = append(result, key)
	}
	return result
}

// DeleteConsumer removes a consumer and its offsets
func DeleteConsumer(consumername string) error {
	consumers.Lock()
	defer consumers.Unlock()

	if _, found := consumers.items[consumername]; !found {
		return nil
	}
	delete(consumers.items, consumername)

	offsets.Lock()
	for key := range offsets.positions {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 && parts[1] == consumername {
			delete(offsets.positions, key)
		}
	}
	offsets.Unlock()

	// Remove from topicConsumers index
	topicConsumers.Lock()
	for _, subs := range topicConsumers.subscribers {
		delete(subs, consumername)
	}
	topicConsumers.Unlock()

	return nil
}

// GetConsumersByTopic returns all consumer IDs subscribed to the given topic
func GetConsumersByTopic(topicname string) ([]string, error) {
	topics.RLock()
	_, exists := topics.items[topicname]
	topics.RUnlock()
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicname)
	}

	topicConsumers.RLock()
	defer topicConsumers.RUnlock()
	subs := topicConsumers.subscribers[topicname]
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
	_, topicExists = topics.items[topic]
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
	topics.RLock()
	topicExists := false
	_, topicExists = topics.items[topic]
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
	topics.RLock()
	topicExists := false
	_, topicExists = topics.items[topic]
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

func GetAllOffsets() map[string]int {
	offsets.RLock()
	defer offsets.RUnlock()
	return offsets.positions
}

func GetAllMessages() map[string][]string {
	messages.RLock()
	defer messages.RUnlock()
	return messages.logs
}

// RestoreStore injects snapshot data by globally locking all core structures
func RestoreStore(newTopics []string, newConsumers []string, newMessages map[string][]string, newOffsets map[string]int) {
	topics.Lock()
	defer topics.Unlock()
	consumers.Lock()
	defer consumers.Unlock()
	messages.Lock()
	defer messages.Unlock()
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

	// Rebuild topicConsumers index from the offsets map ("topic:consumer" keys)
	newSubscribers := make(map[string]map[string]struct{})
	for _, t := range newTopics {
		newSubscribers[t] = make(map[string]struct{})
	}
	for key := range newOffsets {
		for _, t := range newTopics {
			prefix := t + ":"
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				consumerID := key[len(prefix):]
				newSubscribers[t][consumerID] = struct{}{}
			}
		}
	}

	topics.items = newTopicItems
	consumers.items = newConsumerItems
	messages.logs = newMessages
	offsets.positions = newOffsets
	topicConsumers.subscribers = newSubscribers
}

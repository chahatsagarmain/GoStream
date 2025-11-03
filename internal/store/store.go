package store

import (
    "fmt"
    "log"

    "github.com/chahatsagarmain/GoStream/internal/memstore"
)

// in-memory implementation using Go data structures

func CreateTopics(topicname string) error {
    return memstore.CreateTopic(topicname)
}

func GetTopics() ([]string, error) {
    topics := memstore.GetTopics()
    return topics, nil
}

func DeleteTopic(topicname string) error {
    return memstore.DeleteTopic(topicname)
}

func CreateConsumer(consumer string, topicname string) error {
    return memstore.CreateConsumer(consumer, topicname)
}

func GetConsumers() ([]string, error) {
    consumers := memstore.GetConsumers()
    return consumers, nil
}

func DeleteConsumer(consumername string) error {
    return memstore.DeleteConsumer(consumername)
}

func CheckIfTopicsExists(topicname string) (bool, error) {
    topics := memstore.GetTopics()
    for _, t := range topics {
        if t == topicname {
            return true, nil
        }
    }
    return false, nil
}

func CheckIfConsumersExists(consumername string) (bool, error) {
    consumers := memstore.GetConsumers()
    for _, c := range consumers {
        if c == consumername {
            return true, nil
        }
    }
    return false, nil
}

func SetOffset(consumerName string, topicName string, value int) error {
    found, err := CheckIfTopicsExists(topicName)
    if err != nil {
        return err
    }
    if !found {
        return fmt.Errorf("topicname does not exist")
    }

    log.Printf("setting offset for %s:%s as %v", topicName, consumerName, value)
    return memstore.SetOffset(consumerName, topicName, value)
}

func GetOffset(consumerName string, topicName string) (int, error) {
    found, err := CheckIfTopicsExists(topicName)
    if err != nil {
        return -1, err
    }
    if !found {
        return -1, fmt.Errorf("topicname does not exist")
    }

    return memstore.GetOffset(consumerName, topicName)
}

func AppendToLog(topicname string, message string) error {
    found, err := CheckIfTopicsExists(topicname)
    if err != nil {
        return err
    }
    if !found {
        return fmt.Errorf("topic not found")
    }

    return memstore.AppendToLog(topicname, message)
}

func GetMessageFromLog(consumer string, topicname string) (string, error) {
    return memstore.GetMessageFromLog(consumer, topicname)
}
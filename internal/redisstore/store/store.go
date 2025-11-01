package store

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/chahatsagarmain/GoKafka/internal/redisstore"
)

const topickey = "topic"
const consumerkey = "consumer"

// redis list -> topics , consumer , topics:logs , topic_name:messages (Append log)
// redis map -> topic_name:consumer_name : offset , topic_name:consumers : "consumer with delimiter '|' "

func CreateTopics(topicname string) (error){
	ctx := context.Background()

	values , err := redisstore.Client.LRange(ctx , topickey , 0 , -1).Result()
	if err != nil {
		return err
	}
	exists := false
	for _ , key:= range values {
		if key == topicname {
			exists = true
			break
		}
	}

	if !exists {
		if _ , err := redisstore.Client.RPush(ctx , topickey , topicname).Result() ; err != nil {
			return err
		}
	}

	return nil
}

func GetTopics() ([]string , error){
	ctx := context.Background()
	topics , err := redisstore.Client.LRange(ctx , topickey , 0 , -1).Result()
	if err != nil {
		return nil , err
	}
	return topics , nil
}

func DeleteTopic(topicname string) (error) {
	found , err := CheckIfTopicsExists(topicname)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	ctx := context.Background()
	// delete from all data structures 
	// delete from topics list 
	if _ , err := redisstore.Client.LRem(ctx , topickey , 1,  topicname).Result() ; err != nil {
		return err
	}

	pattern := fmt.Sprintf("%s*",topicname)
	iter := redisstore.Client.Scan(ctx , 0 , pattern , 0).Iterator()
	for iter.Next(ctx) {
		if err := redisstore.Client.Del(ctx , iter.Val()).Err() ; err != nil {
			return err
		}
	}

	return nil
}

func CreateConsumer(consumer string , topicname string) (error){
	ctx := context.Background()

	values , err := redisstore.Client.LRange(ctx , consumerkey , 0 , -1).Result()
	if err != nil {
		return err
	}
	exists := false
	for _ , key:= range values {
		if key == consumer {
			exists = true
			break
		}
	}

	if !exists {
		if _ , err := redisstore.Client.RPush(ctx , consumerkey , consumer).Result() ; err != nil {
			return err
		}
	}

	searchKey := fmt.Sprintf("%s:consumers",topickey)
	values , err = redisstore.Client.LRange(ctx , searchKey , 0 , -1).Result()
	if err != nil {
		return err
	}
	exists = false
	for _ , key := range values {
		if key == searchKey {
			exists = true
			break
		}
	}

	if !exists {
		if _ , err := redisstore.Client.RPush(ctx , searchKey , consumer).Result() ; err != nil {
			return err
		}
	}

	if err = SetOffset(consumer , topicname , 0) ; err != nil {
		return err
	}

	return nil
}

func GetConsumers() ([]string , error){
	ctx := context.Background()
	consumers , err := redisstore.Client.LRange(ctx , consumerkey , 0 , -1).Result()
	if err != nil {
		return nil , err
	}
	return consumers , nil
}

func DeleteConsumer(consumername string) (error){
	found , err := CheckIfConsumersExists(consumername)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	ctx := context.Background()
	// delete from all data structures 
	// delete from topics list 
	if _ , err := redisstore.Client.LRem(ctx , consumerkey , 1,  consumername).Result() ; err != nil {
		return err
	}

	pattern := fmt.Sprintf("*%s*",consumername)
	iter := redisstore.Client.Scan(ctx , 0 , pattern , 0).Iterator()
	for iter.Next(ctx) {
		if err := redisstore.Client.Del(ctx , iter.Val()).Err() ; err != nil {
			return err
		}
	}
	return nil

}

func CheckIfTopicsExists(topicname string) (bool , error) {
	ctx := context.Background()

	values , err := redisstore.Client.LRange(ctx , topickey , 0 , -1).Result()
	if err != nil {
		return false , err
	}
	exists := false
	for _ , key:= range values {
		if key == topicname {
			exists = true
			break
		}
	}
	return exists , nil
}

func CheckIfConsumersExists(consumername string) (bool , error) {
	ctx := context.Background()

	values , err := redisstore.Client.LRange(ctx , consumername , 0 , -1).Result()
	if err != nil {
		return false , err
	}
	exists := false
	for _ , key:= range values {
		if key == consumername {
			exists = true
			break
		}
	}
	return exists , nil
}

// func AssignConsumerToTopic(consumer)

func SetOffset(consumerName string , topicName string , value int) (error) {
	ctx := context.Background()
	found , err := CheckIfTopicsExists(topicName)
	if err != nil{
		return err
	}
	if !found {
		return fmt.Errorf("topicname does not exist")
	}
	keyString := fmt.Sprintf("%s:%s",topicName,consumerName)
	log.Printf("setting offset for %s as %v" , keyString , value)
	if _ , err := redisstore.Client.Set(ctx , keyString , strconv.Itoa(value) , 0).Result() ; err != nil {
		return err
	}
	return nil

}

func GetOffset(consumerName string , topicName string) (int , error){
	ctx := context.Background()
	found , err := CheckIfTopicsExists(topicName)
	if err != nil{
		return -1 , err
	}
	if !found {
		return -1 , fmt.Errorf("topicname does not exist")
	}
	checkString := fmt.Sprintf("%s:%s",topicName,consumerName)
	res , err := redisstore.Client.Get(ctx , checkString).Result()
	if err != nil {
		return -1 , err
	}
	offset , err := strconv.Atoi(res)
	if err != nil{
		return -1 , err
	}
	return offset , nil
}

func AppendToLog(topicname string , message string) error {
	found , err := CheckIfTopicsExists(topicname)
	if err != nil{
		return err
	}
	if !found{
		return fmt.Errorf("topic not found")
	}
	searchKey := fmt.Sprintf("%s:messages" , topicname)
	ctx := context.Background()
	if _ , err := redisstore.Client.RPush(ctx , searchKey , message).Result() ; err != nil {
		return err
	}
	return nil
}

// fetch the message from our append log 
// if offset > len - 1 cant do anything 
// if offset <= len - 1 return the item and incement the offset for the topic:consumer
func GetMessageFromLog(consumer string , topicname string) (string , error){
	offset , err := GetOffset(consumer , topicname)
	if err != nil {
		return "" , err
	}
	ctx := context.Background()
	searchKey := fmt.Sprintf("%s:messages",topicname)
	len , err := redisstore.Client.LLen(ctx , searchKey).Result()
	if err != nil {
		return "" , err
	}
	if int64(offset) > len - 1 {
		return "" , fmt.Errorf("out of bound")
	}
	message , err := redisstore.Client.LIndex(ctx , searchKey , int64(offset)).Result()
	if err != nil {
		return "" , err
	}
	err = SetOffset(consumer , topicname , offset + 1)
	if err != nil {
		return ""  , err
	}
	return message , nil
}
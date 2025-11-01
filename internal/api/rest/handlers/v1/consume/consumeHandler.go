package consume

import (
	"fmt"
	"log"
	"net/http"

	"github.com/chahatsagarmain/GoKafka/internal/redisstore/store"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type getTopicBody struct {
	Topicname string `json:"topicname" binding:"required"`
}

type getTopicAndConsumerBody struct {
	Topicname  string `json:"topicname" binding:"required"`
	Consumerid string `json:"consumerid" binding:"required"`
}

func CreateConsumer(c *gin.Context) {
	body := getTopicBody{}
	consumerId := uuid.New()
	if err := c.ShouldBind(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
		return
	}

	found, err := store.CheckIfTopicsExists(body.Topicname)
	if err != nil {
		log.Printf("internal server error : %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
		return
	}

	if !found {
		if err := store.CreateTopics(body.Topicname); err != nil {
			log.Printf("internal server error : %s", err)
			c.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
			return
		}
	}

	if err := store.CreateConsumer(consumerId.String(), body.Topicname); err != nil {
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"message": "consumer created",
		"consumer":  consumerId.String(),
		"topicname": body.Topicname,
	})
}

func GetConsumers(c *gin.Context) {
	consumers, err := store.GetConsumers()
	if err != nil {
		log.Printf("internal server error : %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"message": consumers})
}

func GetOffset(c *gin.Context) {
	body := getTopicAndConsumerBody{
		Consumerid: c.Query("consumerid"),
		Topicname:  c.Query("topicname"),
	}
	if err := c.ShouldBind(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "invalid request values"})
		return
	}
	offset, err := store.GetOffset(body.Consumerid, body.Topicname)
	fmt.Printf("%v , %v", err, offset)
	fmt.Printf("%v", body)
	if err != nil {
		log.Printf("server error : %s", err)
		c.JSON(http.StatusNotFound, gin.H{"message": "topic does not exist"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"message": "fetched offset",
		"offset": offset})
}

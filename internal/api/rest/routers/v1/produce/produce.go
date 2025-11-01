package produce

import (
	"fmt"
	"net/http"

	"github.com/chahatsagarmain/GoKafka/internal/api/rest/handlers/v1/produce"
	"github.com/gin-gonic/gin"
)

func temp(c *gin.Context) {
	fmt.Printf("hi there")
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func ProduceRoute(v1router *gin.RouterGroup) {
	v1router.GET("/ping", temp)
	v1router.GET("/topic", produce.GetTopics)
	v1router.POST("/topic", produce.CreateTopic)
	v1router.DELETE("/topic", produce.DeleteTopic)
	v1router.POST("/publish", produce.PublishMessage)
	v1router.GET("/message", produce.FetchMessage)
}

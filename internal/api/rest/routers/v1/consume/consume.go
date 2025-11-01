package consume

import (
	"fmt"
	"net/http"

	"github.com/chahatsagarmain/GoKafka/internal/api/rest/handlers/v1/consume"
	"github.com/gin-gonic/gin"
)

func temp(c *gin.Context) {
	fmt.Printf("hi there")
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func ConsumeRoute(v1router *gin.RouterGroup) {
	v1router.GET("/consume/ping", temp)
	v1router.POST("/consumer", consume.CreateConsumer)
	v1router.GET("/consumer", consume.GetConsumers)
	v1router.GET("/offset", consume.GetOffset)
}

package produce

import (
	"log"
	"net/http"

	"github.com/chahatsagarmain/GoKafka/internal/redisstore/store"
	"github.com/gin-gonic/gin"
)

// import (

type getTopicBody struct {
	Topicname string `json:"topicname" binding:"required"`
}

func CreateTopic(c *gin.Context) {
	body := getTopicBody{}
	if err := c.ShouldBind(&body) ; err != nil {
		c.JSON(http.StatusBadRequest , gin.H{"message" : "invalid request body"})
		return
	}

	found , err := store.CheckIfTopicsExists(body.Topicname)
	if err != nil {
		log.Printf("internal server error : %s" , err)
		c.JSON(http.StatusInternalServerError , gin.H{"message" : "internal server error"})
		return
	}

	if !found {
		if err := store.CreateTopics(body.Topicname) ; err != nil {
			log.Printf("internal server error : %s" , err)
			c.JSON(http.StatusInternalServerError , gin.H{"message" : "internal server error"})
			return
		}
		c.JSON(http.StatusAccepted , gin.H{"message" : "topic created",
										"topicname" : body.Topicname,
									})
	} else {
		c.JSON(http.StatusAccepted , gin.H{"message" : "topic already created",
										"topicname" : body.Topicname,
									})
	}
	
} 

func GetTopics(c *gin.Context) {
	topics , err := store.GetTopics()
	if err != nil {
		c.JSON(http.StatusInternalServerError , gin.H{"message" : "internal server error"})
		return
	}

	c.JSON(http.StatusAccepted , gin.H{"message" : topics})
}

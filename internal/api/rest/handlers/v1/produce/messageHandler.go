package produce

import (
	"fmt"
	"log"
	"net/http"

	"github.com/chahatsagarmain/GoStream/internal/store"
	"github.com/gin-gonic/gin"
)

type messageBody struct {
	Message    string `json:"message" binding:"required"`
	Topicname  string `json:"topicname" binding:"required"`
	Consumerid string `json:"consumerid,omitempty"`
}

type publishMessageBody struct {
	Message    string `json:"message" binding:"required"`
	Topicname  string `json:"topicname" binding:"required"`
}


func FetchMessage(c *gin.Context) {
	body := messageBody{
		Consumerid: c.DefaultQuery("consumerid", ""),
		Topicname:  c.DefaultQuery("topicname", ""),
	}
	if body.Consumerid == "" || body.Topicname == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
		return
	}
	message, err := store.GetMessageFromLog(body.Consumerid, body.Topicname)
	if err != nil {
		log.Printf("server error : %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"message": message})
}

func PublishMessage(c *gin.Context) {
	body := publishMessageBody{}
	if err := c.ShouldBind(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
		return
	}
	// if offset , err := store.GetOffset(body.Consumerid , body.Topicname) ; err != nil {
	// log.Printf("internal server error : %s" , err)
	// c.JSON(http.StatusInternalServerError , gin.H{"message" : "internal server error"})
	// }

	if err := store.AppendToLog(body.Topicname, body.Message); err != nil {
		log.Printf("internal server error : %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"message": fmt.Sprintf("message published to topic %s", body.Topicname)})

}

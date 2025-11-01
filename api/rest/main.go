package rest

import (
	"github.com/chahatsagarmain/GoKafka/internal/api/rest/routers"
)
func StartRestAPI(){
	gin := routers.Routers()
	gin.Run(":8080")
}
package routers

import (
	v1 "github.com/chahatsagarmain/GoStream/internal/api/rest/routers/v1"
	"github.com/gin-gonic/gin"
)

func Routers() *gin.Engine {
	router := gin.Default()
	v1.V1RouterGroup(router)
	return router
}

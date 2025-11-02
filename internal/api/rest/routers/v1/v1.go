package v1

import (
	"github.com/chahatsagarmain/GoStream/internal/api/rest/routers/v1/consume"
	"github.com/chahatsagarmain/GoStream/internal/api/rest/routers/v1/produce"
	"github.com/gin-gonic/gin"
)

func V1RouterGroup(router *gin.Engine) {
	v1 := router.Group("/v1")
	produce.ProduceRoute(v1)
	consume.ConsumeRoute(v1)
}

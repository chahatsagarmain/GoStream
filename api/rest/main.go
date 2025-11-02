package rest

import (
	"github.com/chahatsagarmain/GoStream/internal/api/rest/routers"
)

func StartRestAPI() {
	gin := routers.Routers()
	gin.Run(":8080")
}

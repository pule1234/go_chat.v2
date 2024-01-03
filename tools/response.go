package tools

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

const (
	CodeSuccess      = 0
	CodeFail         = 1
	CodeUnknownError = -1
	CodeSessionError = 40000
)

var MsgCodeMsg = map[int]string{
	CodeUnknownError: "unKnow error",
	CodeSuccess:      "success",
	CodeFail:         "fail",
	CodeSessionError: "Session error",
}

// 生成带有自定义状态码、消息和数据的响应，并发送给客户端
func ResponseWithCode(c *gin.Context, msgCode int, msg interface{}, data interface{}) {
	if msg == nil {
		if val, ok := MsgCodeMsg[msgCode]; ok {
			msg = val
		} else {
			msg = MsgCodeMsg[-1]
		}
	}

	//响应发送到客户端
	c.AbortWithStatusJSON(http.StatusOK, gin.H{
		"code":    msgCode,
		"message": msg,
		"data":    data,
	})
}

func FailWithMsg(c *gin.Context, msg interface{}) {
	ResponseWithCode(c, CodeFail, msg, nil)
}

func SuccessWithMsg(c *gin.Context, msg interface{}, data interface{}) {
	ResponseWithCode(c, CodeSuccess, msg, data)
}

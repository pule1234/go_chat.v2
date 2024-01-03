package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/sirupsen/logrus"
	"goim/api/rpc"
	"goim/config"
	"goim/proto"

	"goim/tools"
	"strconv"
)

type FormPush struct {
	Msg       string `form:"msg" json:"msg" binding:"required"`
	ToUserId  string `form:"toUserId" json:"toUserId" binding:"required"`
	RoomId    int    `form:"roomId" json:"roomId" binding:"required"`
	AuthToken string `form:"authToken" json:"authToken" binding:"required"`
}

func Push(c *gin.Context) {
	var formPush FormPush
	if err := c.ShouldBindBodyWith(&formPush, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		logrus.Info("绑定错误 err = ", err)
		return
	}

	authToken := formPush.AuthToken
	msg := formPush.Msg
	toUserId := formPush.ToUserId
	toUserIdInt, _ := strconv.Atoi(toUserId)
	getUserNameReq := &proto.GetUserInfoRequest{UserId: toUserIdInt}
	code, toUserName := rpc.RpcLogicObj.GetUserNameByUserId(getUserNameReq)
	logrus.Info("toUserName = ", toUserName)

	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get friend userName")
		return
	}
	checkAuthReq := &proto.CheckAuthRequest{authToken}

	code, fromUserId, fromUserName := rpc.RpcLogicObj.CheckAuth(checkAuthReq)
	logrus.Infof("code = %d, fromuserid = %d     forusername = %s \n", code, fromUserId, fromUserName)

	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get self info")
		return
	}
	roomId := formPush.RoomId
	req := &proto.Send{
		Msg:          msg,
		FromUserId:   fromUserId,
		FromUserName: fromUserName,
		ToUserId:     toUserIdInt,
		ToUserName:   toUserName,
		RoomId:       roomId,
		Op:           config.OpSingleSend,
	}
	code, rpcMsg := rpc.RpcLogicObj.Push(req)
	if code == tools.CodeFail {
		tools.FailWithMsg(c, rpcMsg)
		return
	}
	tools.SuccessWithMsg(c, "ok", nil)
	return
}

// 此时不需要msg，调用cosupload函数即可，再使用msg来接收腾讯云返回的url
type FormPushFile struct {
	ToUserId  string `form:"toUserId" json:"toUserId" binding:"required"`
	RoomId    int    `form:"roomId" json:"roomId" binding:"required"`
	AuthToken string `form:"authToken" json:"authToken" binding:"required"`
}

func Push_File(c *gin.Context) {
	var formPushfile FormPushFile
	if err := c.ShouldBindBodyWith(&formPushfile, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		logrus.Info("绑定错误 err = ", err)
		return
	}

	authToken := formPushfile.AuthToken
	toUserId := formPushfile.ToUserId
	toUserIdInt, _ := strconv.Atoi(toUserId)
	getUserNameReq := &proto.GetUserInfoRequest{UserId: toUserIdInt}
	code, toUserName := rpc.RpcLogicObj.GetUserNameByUserId(getUserNameReq)
	logrus.Info("toUserName = ", toUserName)

	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get friend userName")
		return
	}
	checkAuthReq := &proto.CheckAuthRequest{authToken}

	code, fromUserId, fromUserName := rpc.RpcLogicObj.CheckAuth(checkAuthReq)
	logrus.Infof("code = %d, fromuserid = %d     forusername = %s \n", code, fromUserId, fromUserName)

	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get self info")
		return
	}

	roomId := formPushfile.RoomId

	msg, err := tools.CosUpload(c)
	logrus.Infof("腾讯云服务器返回的url为 : %s", msg)
	if err != nil {
		logrus.Errorf("上传腾讯云服务器错误: %v", err)
	}

	req := &proto.Send{
		Msg:          msg,
		FromUserId:   fromUserId,
		FromUserName: fromUserName,
		ToUserId:     toUserIdInt,
		ToUserName:   toUserName,
		RoomId:       roomId,
		Op:           config.OpSingleSend_File,
	}

	code, rpcMsg := rpc.RpcLogicObj.Push_File(req)

	if code == tools.CodeFail {
		tools.FailWithMsg(c, rpcMsg)
		return
	}
	tools.SuccessWithMsg(c, "ok", nil)
	return
}

type FormRoom struct {
	AuthToken string `form:"authToken" json:"authToken" binding:"required"`
	Msg       string `form:"msg" json:"msg" binding:"required"`
	RoomId    int    `form:"roomId" json:"roomId" binding:"required"`
}

func PushRoom(c *gin.Context) {
	var formRoom FormRoom
	if err := c.ShouldBindBodyWith(&formRoom, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		return
	}
	authToken := formRoom.AuthToken
	msg := formRoom.Msg
	roomId := formRoom.RoomId
	checkAuthReq := &proto.CheckAuthRequest{authToken}
	authCode, fromUserId, fromUserName := rpc.RpcLogicObj.CheckAuth(checkAuthReq)
	if authCode == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get self info")
		return
	}
	req := &proto.Send{
		Msg:          msg,
		FromUserId:   fromUserId,
		FromUserName: fromUserName,
		RoomId:       roomId,
		Op:           config.OpRoomSend,
	}
	code, msg := rpc.RpcLogicObj.PushRoom(req)
	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc push room msg fail!")
		return
	}
	tools.SuccessWithMsg(c, "ok", msg)
	return
}

type FormRoom_File struct {
	AuthToken string `form:"authToken" json:"authToken" binding:"required"`
	RoomId    int    `form:"roomId" json:"roomId" binding:"required"`
}

func PushRoom_File(c *gin.Context) {
	var formRoom FormRoom
	if err := c.ShouldBindBodyWith(&formRoom, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		return
	}

	authToken := formRoom.AuthToken
	roomId := formRoom.RoomId
	checkAuthReq := &proto.CheckAuthRequest{authToken}
	authCode, fromUserId, fromUserName := rpc.RpcLogicObj.CheckAuth(checkAuthReq)
	if authCode == tools.CodeFail {
		tools.FailWithMsg(c, "rpc fail get self info")
		return
	}

	msg, err := tools.CosUpload(c)
	logrus.Infof("群聊：云服务器返回的url为: %s", msg)
	if err != nil {
		logrus.Errorf("群聊，上传腾讯云服务器失败")
	}

	req := &proto.Send{
		Msg:          msg,
		FromUserId:   fromUserId,
		FromUserName: fromUserName,
		RoomId:       roomId,
		Op:           config.OpRoomSend_File,
	}
	code, msg := rpc.RpcLogicObj.PushRoom_File(req)
	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc push room msg fail!")
		return
	}
	tools.SuccessWithMsg(c, "ok", msg)
	return
}

type FormCount struct {
	RoomId int `form:"roomId" json:"roomId" binding:"required"`
}

func Count(c *gin.Context) {
	var formCount FormCount
	if err := c.ShouldBindWith(&formCount, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		fmt.Println("绑定失败")
		return
	}
	roomId := formCount.RoomId
	req := &proto.Send{
		RoomId: roomId,
		Op:     config.OpRoomCountSend,
	}
	code, msg := rpc.RpcLogicObj.Count(req)
	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc get room count fail!")
		return
	}
	tools.SuccessWithMsg(c, "ok", msg)
	return
}

type FormRoomInfo struct {
	RoomId int `form:"roomId" json:"roomId" binding:"required"`
}

func GetRoomInfo(c *gin.Context) {
	var formRoomInfo FormRoomInfo
	if err := c.ShouldBindWith(&formRoomInfo, binding.JSON); err != nil {
		tools.FailWithMsg(c, err.Error())
		return
	}
	roomId := formRoomInfo.RoomId
	req := &proto.Send{
		RoomId: roomId,
		Op:     config.OpRoomInfoSend,
	}
	code, msg := rpc.RpcLogicObj.GetRoomInfo(&req)
	if code == tools.CodeFail {
		tools.FailWithMsg(c, "rpc get room info fail!")
		return
	}
	tools.SuccessWithMsg(c, "ok", msg)
	return
}

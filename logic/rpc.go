package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"goim/config"
	"goim/logic/dao"
	"goim/proto"
	"goim/tools"
	"strconv"
)

type RpcLogic struct {
}

// 用于处理注册逻辑
func (rpc *RpcLogic) Register(ctx context.Context, args *proto.RegisterRequest, reply *proto.RegisterReply) (err error) {
	reply.Code = config.FailReplyCode
	u := new(dao.User)
	uData := u.CheckHaveUserName(args.Name)
	if uData.Id > 0 {
		return errors.New("this user name already have , please login !!!")
	}
	u.UserName = args.Name
	u.Password = args.Password
	userId, err := u.Add()
	if err != nil {
		logrus.Infof("register err:%s", err.Error())
		return err
	}
	if userId == 0 {
		return errors.New("register userId empty!")
	}
	// set token
	randToken := tools.GetRandomToken(32)
	sessionId := tools.CreateSessionId(randToken)
	userData := make(map[string]interface{})
	userData["userId"] = userId
	userData["userName"] = args.Name
	//开启一个事务
	RedisSessClient.Send(ctx, "MULTI")

	// 将多个命令添加到事务队列中
	RedisSessClient.Send(ctx, "HMSET", sessionId, userData)
	RedisSessClient.Send(ctx, "EXPIRE", sessionId, 86400)
	err = RedisSessClient.Flush(ctx)
	if err != nil {
		logrus.Infof(" register set redis token fail!")
		return err
	}
	_, err = RedisSessClient.Receive(ctx)
	//err = RedisSessClient.Set(authToken, data.Id, 86400*time.Second)
	if err != nil {
		logrus.Infof("register set redis token fail!")
		return err
	}

	reply.Code = config.SuccessReplyCode
	reply.AuthToken = randToken
	return
}

func (rpc *RpcLogic) Login(ctx context.Context, args *proto.LoginRequest, reply *proto.LoginResponse) (err error) {
	reply.Code = config.FailReplyCode
	u := new(dao.User)
	userName := args.Name
	passWord := args.Password
	data := u.CheckHaveUserName(userName)
	if (data.Id == 0) || (passWord != data.Password) {
		return errors.New("no this user or password error!")
	}

	loginSessionId := tools.GetSessionIdByUserId(data.Id)
	//set token
	//err = redis.HMSet(auth, userData)
	randToken := tools.GetRandomToken(32)
	sessionId := tools.CreateSessionId(randToken)
	userData := make(map[string]interface{})
	userData["userId"] = data.Id
	userData["userName"] = data.UserName
	//check is login如果获取到的令牌不为空，则表示已经有用户登录了
	token, _ := RedisSessClient.Get(ctx, loginSessionId)
	if token != "" {
		//删除原有的会话id
		oldSession := tools.CreateSessionId(token)
		err := RedisSessClient.Del(ctx, oldSession)
		if err != nil {
			return errors.New("logout user fail!token is:" + token)
		}
	}
	RedisSessClient.Send(ctx, "MULTI")

	// 将多个命令添加到事务队列中
	RedisSessClient.Send(ctx, "HMSET", sessionId, userData)
	RedisSessClient.Send(ctx, "EXPIRE", sessionId, 86400)
	RedisSessClient.Send(ctx, "SET", loginSessionId, randToken)
	RedisSessClient.Send(ctx, "EXPIRE", loginSessionId, 86400)
	err = RedisSessClient.Flush(ctx)
	if err != nil {
		logrus.Infof(" register set redis token fail!")
		return err
	}
	_, err = RedisSessClient.Receive(ctx)
	//err = RedisSessClient.Set(authToken, data.Id, 86400*time.Second)
	if err != nil {
		logrus.Infof("register set redis token fail!")
		return err
	}
	reply.Code = config.SuccessReplyCode
	reply.AuthToken = randToken
	logrus.Info("reply.AuthToken = ", reply.AuthToken)
	return
}

func (rpc *RpcLogic) GetUserInfoByUserId(ctx context.Context, args *proto.GetUserInfoRequest, reply *proto.GetUserInfoResponse) (err error) {
	reply.Code = config.FailReplyCode
	userId := args.UserId
	u := new(dao.User)
	userName := u.GetUserNameByUserId(userId)
	reply.UserId = userId
	reply.UserName = userName
	reply.Code = config.SuccessReplyCode
	return
}

func (rpc *RpcLogic) CheckAuth(ctx context.Context, args *proto.CheckAuthRequest, reply *proto.CheckAuthResponse) (err error) {
	reply.Code = config.FailReplyCode
	authToken := args.AuthToken
	sessionName := tools.GetSessionName(authToken)

	var userDataMap = map[string]string{}
	userDataMap, err = RedisSessClient.HGetAll(ctx, sessionName)
	if err != nil {
		logrus.Infof("check auth fail!,authToken is:%s", authToken)
		return err
	}

	if len(userDataMap) == 0 {
		logrus.Infof("no this user session,authToken is:%s", authToken)
		return
	} else {
		logrus.Info("不等于0")
	}

	intUserId, _ := strconv.Atoi(userDataMap["userId"])
	reply.UserId = intUserId
	userName, _ := userDataMap["userName"]
	reply.Code = config.SuccessReplyCode
	logrus.Info("reply COde = ", reply.Code)
	reply.UserName = userName
	logrus.Info("reply username = ", reply.UserName)
	return
}

func (rpc *RpcLogic) Logout(ctx context.Context, args *proto.LogoutRequest, reply *proto.LogoutResponse) (err error) {
	reply.Code = config.FailReplyCode
	authToken := args.AuthToken
	sessionName := tools.GetSessionName(authToken)

	var userDataMap = map[string]string{}
	userDataMap, err = RedisSessClient.HGetAll(ctx, sessionName)
	if err != nil {
		logrus.Infof("check auth fail!,authToken is:%s", authToken)
		return err
	}
	if len(userDataMap) == 0 {
		logrus.Infof("no this user session,authToken is:%s", authToken)
		return
	}

	intUserId, _ := strconv.Atoi(userDataMap["userId"])
	sessIdMap := tools.GetSessionIdByUserId(intUserId)
	//del sess_map like sess_map_1
	err = RedisSessClient.Del(ctx, sessIdMap)
	if err != nil {
		logrus.Infof("logout del sess map error:%s", err.Error())
		return err
	}
	//del serverId
	logic := new(Logic)
	serverIdKey := logic.GetUserKey(fmt.Sprintf("%d", intUserId))
	err = RedisSessClient.Del(ctx, serverIdKey)
	if err != nil {
		logrus.Infof("logout del server id error:%s", err.Error())
		return err
	}
	err = RedisSessClient.Del(ctx, sessionName)
	if err != nil {
		logrus.Infof("logout error:%s", err.Error())
		return err
	}
	reply.Code = config.SuccessReplyCode
	return
}

/*
*
single send msg
*/
func (rpc *RpcLogic) Push(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	sendData := args
	var bodyBytes []byte
	bodyBytes, err = json.Marshal(sendData)
	if err != nil {
		logrus.Errorf("logic,push msg fail,err:%s", err.Error())
		return
	}

	logic := new(Logic)
	//获取需要到达的id
	userSidKey := logic.GetUserKey(fmt.Sprintf("%d", sendData.ToUserId))
	serverIdStr, err := RedisSessClient.Get(ctx, userSidKey)
	if err != nil {
		logrus.Errorf("logic,push parse int fail:%s", err.Error())
		return
	}
	err = logic.RedisPublishChannel(serverIdStr, sendData.ToUserId, bodyBytes)
	if err != nil {
		logrus.Errorf("logic,redis publish err: %s", err.Error())
		return
	}
	reply.Code = config.SuccessReplyCode
	return
}

/*
*
push msg to room
*/
func (rpc *RpcLogic) Push_File(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	sendData := args
	var bodyBytes []byte
	bodyBytes, err = json.Marshal(sendData)
	if err != nil {
		logrus.Errorf("logic,push msg fail,err:%s", err.Error())
		return
	}

	logic := new(Logic)
	userSidKey := logic.GetUserKey(fmt.Sprintf("%d", sendData.ToUserId))
	serverIdStr, err := RedisSessClient.Get(ctx, userSidKey)

	if err != nil {
		logrus.Errorf("logic,push parse int fail:%s", err.Error())
		return
	}
	err = logic.RedisPublishChannel_File(serverIdStr, sendData.ToUserId, bodyBytes)
	if err != nil {
		logrus.Errorf("logic,redis publish err: %s", err.Error())
		return
	}

	reply.Code = config.SuccessReplyCode
	return
}

/*
*
push msg to room
*/

func (rpc *RpcLogic) PushRoom(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	sendData := args
	roomId := sendData.RoomId
	logic := new(Logic)
	roomUserInfo := make(map[string]string)
	roomUserKey := logic.getRoomUserKey(strconv.Itoa(roomId))
	// 获取房间
	roomUserInfo, err = RedisClient.HGetAll(ctx, roomUserKey)
	if err != nil {
		logrus.Errorf("logic,PushRoom redis hGetAll err:%s", err.Error())
		return
	}
	var bodyBytes []byte
	sendData.RoomId = roomId
	sendData.Msg = args.Msg
	sendData.FromUserName = args.FromUserName
	sendData.FromUserId = args.FromUserId
	sendData.Op = config.OpRoomSend
	sendData.CreateTime = tools.GetNowDateTime()
	bodyBytes, err = json.Marshal(sendData)
	if err != nil {
		logrus.Errorf("logic,PushRoom Marshal err:%s", err.Error())
		return
	}
	err = logic.RedisPublishRoomInfo(roomId, len(roomUserInfo), roomUserInfo, bodyBytes)
	if err != nil {
		logrus.Errorf("logic,PushRoom err:%s", err.Error())
		return
	}
	reply.Code = config.SuccessReplyCode
	return
}

func (rpc *RpcLogic) PushRoom_File(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	sendData := args
	roomId := sendData.RoomId
	logic := new(Logic)
	roomUserInfo := make(map[string]string)
	roomUserKey := logic.getRoomUserKey(strconv.Itoa(roomId))
	// 获取房间
	roomUserInfo, err = RedisClient.HGetAll(ctx, roomUserKey)
	if err != nil {
		logrus.Errorf("logic,PushRoom redis hGetAll err:%s", err.Error())
		return
	}
	var bodyBytes []byte
	sendData.RoomId = roomId
	sendData.Msg = args.Msg
	sendData.FromUserName = args.FromUserName
	sendData.FromUserId = args.FromUserId
	sendData.Op = config.OpRoomSend_File
	sendData.CreateTime = tools.GetNowDateTime()
	bodyBytes, err = json.Marshal(sendData)
	if err != nil {
		logrus.Errorf("logic,PushRoom Marshal err:%s", err.Error())
		return
	}
	err = logic.RedisPublishRoomInfo_File(roomId, len(roomUserInfo), roomUserInfo, bodyBytes)
	if err != nil {
		logrus.Errorf("logic,PushRoom err:%s", err.Error())
		return
	}
	reply.Code = config.SuccessReplyCode
	return
}

/*
*
get room online person count
*/
func (rpc *RpcLogic) Count(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	roomId := args.RoomId
	logic := new(Logic)
	var count string
	count, err = RedisSessClient.Get(ctx, logic.getRoomOnlineCountKey(fmt.Sprintf("%d", roomId)))
	atoi, err := strconv.Atoi(count)
	if err != nil {
		return err
	}
	err = logic.RedisPushRoomCount(roomId, atoi)
	if err != nil {
		logrus.Errorf("logic,Count err:%s", err.Error())
		return
	}
	reply.Code = config.SuccessReplyCode
	return
}

func (rpc *RpcLogic) GetRoomInfo(ctx context.Context, args *proto.Send, reply *proto.SuccessReply) (err error) {
	reply.Code = config.FailReplyCode
	logic := new(Logic)
	roomId := args.RoomId
	roomUserInfo := make(map[string]string)
	roomUserKey := logic.getRoomUserKey(strconv.Itoa(roomId))
	roomUserInfo, err = RedisClient.HGetAll(ctx, roomUserKey)
	if len(roomUserInfo) == 0 {
		return errors.New("getRoomInfo no this user")
	}
	err = logic.RedisPushRoomInfo(roomId, len(roomUserInfo), roomUserInfo)
	if err != nil {
		logrus.Errorf("logic,GetRoomInfo err:%s", err.Error())
		return
	}
	reply.Code = config.SuccessReplyCode
	return
}

// 用于处理连接请求的RPC方法
func (rpc *RpcLogic) Connect(ctx context.Context, args *proto.ConnectRequest, reply *proto.ConnectReply) (err error) {
	if args == nil {
		logrus.Errorf("logic,connect args empty")
		return
	}

	logic := new(Logic)
	//key := logic.getUserKey(args.AuthToken)
	logrus.Infof("logic,authToken is:%s", args.AuthToken)
	//根据args中的认证令牌（args.AuthToken）使用tools.GetSessionName()方法生成对应的会话键名（key）。
	key := tools.GetSessionName(args.AuthToken)
	//使用key在redis中查找相应的信息
	userInfo, err := RedisClient.HGetAll(ctx, key)
	if err != nil {
		logrus.Infof("RedisCli HGetAll key :%s , err:%s", key, err.Error())
		return err
	}
	if len(userInfo) == 0 {
		reply.UserId = 0
		return
	}
	reply.UserId, _ = strconv.Atoi(userInfo["userId"])
	//args.RoomId）使用logic.getRoomUserKey()方法生成房间用户信息的键名（roomUserKey）
	roomUserKey := logic.getRoomUserKey(strconv.Itoa(args.RoomId))

	if reply.UserId != 0 { //表示找到了与 key 相关的用户信息
		//使用logic.getUserKey()方法根据reply.UserId生成用户键名（userKey）
		userKey := logic.GetUserKey(fmt.Sprintf("%d", reply.UserId))

		logrus.Infof("logic redis set userKey:%s, serverId : %s", userKey, args.ServerId)
		//validTime := config.RedisBaseValidTime * time.Second

		//将args.ServerId存储到Redis中的userKey对应的值，并设置过期时间为config.RedisBaseValidTime秒。
		// 记录用户的服务器id  ，更新用户连接的服务器id

		_, err = RedisClient.Set(ctx, userKey, args.ServerId)

		if err != nil {
			logrus.Warnf("logic set err:%s", err)
		}

		//表示用户尚未加入到该房间
		str, err := RedisClient.HGet(ctx, roomUserKey, fmt.Sprintf("%d", reply.UserId))
		if err != nil {
			logrus.Errorf("HGet err :", err)
			return err
		}
		if str == "" {
			RedisClient.HSet(ctx, roomUserKey, fmt.Sprintf("%d", reply.UserId), userInfo["userName"])
			// add room user count ++使用RedisClient.Incr()方法将房间在线用户数量计数器递增。
			RedisClient.Incr(ctx, logic.getRoomOnlineCountKey(fmt.Sprintf("%d", args.RoomId)))
		}
	}
	logrus.Infof("logic rpc userId:%d", reply.UserId)
	return
}

func (rpc *RpcLogic) DisConnect(ctx context.Context, args *proto.DisConnectRequest, reply *proto.DisConnectReply) (err error) {
	logic := new(Logic)
	//根据args中的房间ID（args.RoomId）使用logic.getRoomUserKey()方法生成房间用户信息的键名（roomUserKey）
	roomUserKey := logic.getRoomUserKey(strconv.Itoa(args.RoomId))
	if args.RoomId > 0 {
		//使用logic.getRoomOnlineCountKey()方法生成房间在线用户数量的键名
		count, _ := RedisClient.Get(ctx, logic.getRoomOnlineCountKey(fmt.Sprintf("%d", args.RoomId)))
		atoi, err := strconv.Atoi(count)
		if err != nil {
			return err
		}
		if atoi > 0 { //-1
			RedisClient.Decr(ctx, logic.getRoomOnlineCountKey(fmt.Sprintf("%d", args.RoomId)))
		}
	}

	if args.UserId != 0 {
		//使用RedisClient.HDel()方法从roomUserKey对应的哈希表中删除args.UserId对应的字段。
		err = RedisClient.HDel(ctx, roomUserKey, fmt.Sprintf("%d", args.UserId))
		if err != nil {
			logrus.Warnf("HDel getRoomUserKey err : %s", err)
		}
	}
	//below code can optimize send a signal to queue,another process get a signal from queue,then push event to websocket
	//获取roomUserKey对应的哈希表中的所有字段和值  房间中剩余的用户信息
	roomUserInfo, err := RedisClient.HGetAll(ctx, roomUserKey)
	if err != nil {
		logrus.Warnf("RedisCli HGetAll roomUserInfo key:%s, err: %s", roomUserKey, err)
	}
	// 更新 房间信息
	if err = logic.RedisPublishRoomInfo(args.RoomId, len(roomUserInfo), roomUserInfo, nil); err != nil {
		logrus.Warnf("publish RedisPublishRoomCount err: %s", err.Error())
		return
	}
	return
}

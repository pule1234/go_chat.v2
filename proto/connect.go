package proto

type Msg struct {
	Ver       int    `json:"ver"`  //protocol version
	Operation int    `json:"op"`   //operation for request
	SeqId     string `json:"seq"`  //客户端选择的序列号
	Body      []byte `json:"body"` //binary body bytes
}

type PushMsgRequest struct {
	UserId int
	Msg    Msg
}

type PushRoomMsgRequest struct {
	RoomId int
	Msg    Msg
}

type PushRoomCountRequest struct {
	RoomId int
	Count  int
}

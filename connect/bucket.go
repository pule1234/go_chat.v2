package connect

import (
	"github.com/sirupsen/logrus"
	"goim/proto"
	"sync"
	"sync/atomic"
)

// 在链接层减小锁竞争，会进行用户userid分桶来降低竞争
type Bucket struct {
	cLock        sync.RWMutex
	chs          map[int]*Channel
	bucketOptios BucketOptions
	rooms        map[int]*Room
	routines     []chan *proto.PushRoomMsgRequest
	routinesNum  uint64
	broadcast    chan []byte
}

type BucketOptions struct {
	ChannelSize   int
	RoomSize      int
	RoutineAmount uint64
	RoutineSize   int
}

func NewBucket(bucketOptions BucketOptions) (b *Bucket) {
	b = new(Bucket)
	b.chs = make(map[int]*Channel, bucketOptions.ChannelSize)
	b.bucketOptios = bucketOptions
	b.routines = make([]chan *proto.PushRoomMsgRequest, bucketOptions.RoutineAmount)
	b.rooms = make(map[int]*Room, bucketOptions.RoomSize)

	for i := uint64(0); i < b.bucketOptios.RoutineAmount; i++ {
		//rountine
		c := make(chan *proto.PushRoomMsgRequest, bucketOptions.RoutineSize)
		b.routines[i] = c
		//将接收到的消息推送给对应房间的通道，实现了将消息分发到对应房间中的功能
		go b.PushRoom(b.routines[i])
	}
	return
}

func (b *Bucket) PushRoom(ch chan *proto.PushRoomMsgRequest) {
	for {
		var (
			arg  *proto.PushRoomMsgRequest
			room *Room
		)
		arg = <-ch
		logrus.Info("roomid ", arg.RoomId)
		if room = b.Room(arg.RoomId); room != nil {
			room.Push(&arg.Msg) //将消息推送给channel.broadcast
		}
	}
}

func (b *Bucket) Room(rid int) (room *Room) {
	b.cLock.Lock()
	room, _ = b.rooms[rid]
	b.cLock.Unlock()
	return
}

func (b *Bucket) DeleteChannel(ch *Channel) {
	var (
		ok   bool
		room *Room
	)
	b.cLock.RLock()
	if ch, ok = b.chs[ch.userId]; ok {
		room = b.chs[ch.userId].Room
		delete(b.chs, ch.userId)
	}
	if room != nil && room.DeleteChannel(ch) {
		// if room empty delete,will mark room.drop is true
		if room.drop == true {
			delete(b.rooms, room.Id)
		}
	}
	b.cLock.RUnlock()
}

func (b *Bucket) Put(userId int, roomId int, ch *Channel) (err error) {
	var (
		room *Room
		ok   bool
	)
	b.cLock.Lock()
	if roomId != NoRoom {
		if room, ok = b.rooms[roomId]; !ok {
			room = NewRoom(roomId)
			b.rooms[roomId] = room
		}
		ch.Room = room
	}
	ch.userId = userId
	b.chs[userId] = ch
	b.cLock.Unlock()

	if room != nil {
		//// 加入Channel到Room的双向链表中的。
		err = room.Put(ch)
	}

	return
}

func (b *Bucket) Channel(UserId int) (ch *Channel) {
	b.cLock.RLock()
	ch = b.chs[UserId]
	b.cLock.RUnlock()
	return
}

// 将pushRoomMsgReq消息广播到指定的房间。
func (b *Bucket) BroadcastRoom(pushRoomMsgReq *proto.PushRoomMsgRequest) {
	//自增后取模
	num := atomic.AddUint64(&b.routinesNum, 1) % b.bucketOptios.RoutineAmount
	b.routines[num] <- pushRoomMsgReq
	logrus.Info(b.routines[num])
}

package connect

import (
	"errors"
	"github.com/sirupsen/logrus"
	"goim/proto"
	"sync"
)

const NoRoom = -1

type Room struct {
	Id          int
	OnlineCount int
	rLock       sync.RWMutex
	drop        bool     // make room is live
	next        *Channel // 用于连接channel节点   指向双向链表的尾部
}

// 将消息 msg 推送给当前房间（Room）中的所有通道（Channel）。
func (r *Room) Push(msg *proto.Msg) {
	r.rLock.RLock()
	for ch := r.next; ch != nil; ch = ch.Next {
		if err := ch.Push(msg); err != nil {
			logrus.Infof("push msg err:%s", err.Error())
		}
	}
	r.rLock.RUnlock()
	return
}

// 从双向链表中删除ch
func (r *Room) DeleteChannel(ch *Channel) bool {
	r.rLock.Lock()

	if ch.Next != nil {
		ch.Next.Prev = ch.Prev
	}
	if ch.Prev != nil {
		ch.Prev.Next = ch.Next
	} else {
		r.next = ch.Next
	}

	r.OnlineCount--
	r.drop = false

	if r.OnlineCount <= 0 {
		r.drop = true
	}
	r.rLock.RUnlock()
	return r.drop
}

func NewRoom(roomId int) *Room {
	room := new(Room)
	room.Id = roomId
	room.drop = false
	room.next = nil
	room.OnlineCount = 0
	return room
}

// 加入Channel到Room的双向链表中的。
func (r *Room) Put(ch *Channel) (err error) {
	//doubly linked list
	r.rLock.Lock()
	defer r.rLock.Unlock()
	if !r.drop {
		if r.next != nil {
			r.next.Prev = ch
		}
		ch.Next = r.next
		ch.Prev = nil
		r.next = ch
		r.OnlineCount++
	} else {
		err = errors.New("room drop")
	}
	return
}

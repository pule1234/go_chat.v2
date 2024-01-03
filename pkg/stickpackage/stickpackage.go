package stickpackage

import (
	"encoding/binary"
	"fmt"
	"io"
)

// 实现tcp拆包解包是用到
//主要原因是tcp是基于第4层的流式协议而非应用层协议，所以才有了这个过程。

var VersionLenth = 2
var LengthLength = 2
var TcpHeaderLength = VersionLenth + LengthLength
var LengthStartIndex = 2 // 数据部分长度起始字节位置
var LengthStopIndex = 4  // 数据部分长度结束字节位置
var VersionContent = [2]byte{'v', '1'}

type StickPackage struct {
	Version [2]byte // 长度为两个字节
	Length  int16   //长度为两个字节，代表msg的长度
	Msg     []byte  // 需要根据Length字段的值确定长度
}

// 用于将StickPackage对象打包成二进制数据，并写入到io.Writer中
func (p *StickPackage) Pack(writer io.Writer) error {
	var err error
	//数据写入到writer中
	err = binary.Write(writer, binary.BigEndian, &p.Version)
	err = binary.Write(writer, binary.BigEndian, &p.Length)
	err = binary.Write(writer, binary.BigEndian, p.Msg)
	return err
}

func (p *StickPackage) Unpack(reader io.Reader) error {
	var err error
	//数据写入到version中
	err = binary.Read(reader, binary.BigEndian, &p.Version)
	err = binary.Read(reader, binary.BigEndian, &p.Length)
	p.Msg = make([]byte, p.Length-4)
	err = binary.Read(reader, binary.BigEndian, &p.Msg)
	return err
}

func (p *StickPackage) String() string {
	return fmt.Sprintf("version:%s length:%d msg:%s",
		p.Version,
		p.Length,
		p.Msg,
	)
}

func (p *StickPackage) GetPackageLength() int16 {
	p.Length = 4 + int16(len(p.Msg))
	return p.Length
}

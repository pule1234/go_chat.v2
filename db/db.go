package db

import (
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var dbMap = map[string]*gorm.DB{}

func init() {
	initDB("gochat")
}

func initDB(dbName string) {
	//var e error
	//dbConfig := config.Conf.Db
	//dbMap[dbName], e = gorm.Open("mysql", dbConfig.DbBase.Link)
	////dbMap[dbName].DB().SetMaxIdleConns(4)  //连接池初始数
	////dbMap[dbName].DB().SetMaxOpenConns(20) //连接池最大数
	////dbMap[dbName].DB().SetConnMaxLifetime(8 * time.Second)
	//if e != nil {
	//	logrus.Errorf("connect db fail:%s", e.Error())
	//}
	var err error
	dsn := "root:1234@tcp(127.0.0.1:3306)/gochat?charset=utf8mb4&parseTime=True&loc=Local"
	dbMap[dbName], err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logrus.Errorf("connect db fail:%s", err.Error())
	}
}

func GetDb(dbName string) (db *gorm.DB) {
	if db, ok := dbMap[dbName]; ok {
		return db
	} else {
		return nil
	}
}

type DbGoChat struct {
}

func (*DbGoChat) GetDbName() string {
	return "gochat"
}

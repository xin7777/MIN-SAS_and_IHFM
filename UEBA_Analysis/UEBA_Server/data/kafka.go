package data

import (
	"log"
	"time"
)


/**
	生成推送信息
 */
func GetPushMsg(c chan string)  {
	for {
		log.Println("message was successfully created")
		c <- "5Lya6K6u55S16ISR"
		time.Sleep(5 * time.Second)
	}
}
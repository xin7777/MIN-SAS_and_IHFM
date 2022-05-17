package web

import (
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"../data"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize: 1024,
	WriteBufferSize: 1024,
}

/**
	负责向网页端推送推送信息
 */
func sender(conn *websocket.Conn)  {
	c := make(chan string)
	go data.GetPushMsg(c)
	for {
		// log.Println(string(p))
		msg := <-c
		if err := conn.WriteMessage(1, []byte(msg)); err != nil {
			log.Println(err)
			return
		}

		log.Println("message was successfully sent")
		// time.Sleep(5)
	}
}

/**
	WebSocket Handler
 */
func WSEndPoint(w http.ResponseWriter, r *http.Request)  {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil{
		log.Println(err)
	}

	log.Println("Client successfully connected...")

	sender(ws)

	defer ws.Close()
}
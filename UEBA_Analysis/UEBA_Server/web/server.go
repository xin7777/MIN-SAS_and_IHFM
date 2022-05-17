package web

import (
	"log"
	"net/http"
)


func setupRoutes() {
	http.HandleFunc("/ws", WSEndPoint)
}

func init()  {
	log.Println("Setting up routes")
	setupRoutes()
}

func Run()  {
	log.Println("Server started")
	log.Fatal(http.ListenAndServe(":5056", nil))
}

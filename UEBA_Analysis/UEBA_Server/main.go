package main

import (
	"./web"
	"log"
)

func init() {
	log.Println("hello from init go")
}

func main() {
	web.Run()
}

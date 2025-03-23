package main

import (
	"log"
	"net"

	fluxdb "github.com/saivenkatram-git/fluxdb/config"
)

func main() {

	fluxdb := fluxdb.New()

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("FluxDB listening on port 6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go fluxdb.HandleConnection(conn)
	}

}

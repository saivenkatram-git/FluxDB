package main

import (
	"fmt"
	"log"
	"net"

	fluxdb "github.com/saivenkatram-git/fluxdb/config"
)

func main() {

	fluxdb := fluxdb.New()

	port := fluxdb.GetConfig("port")
	bind := fluxdb.GetConfig("bind")
	addr := fmt.Sprintf("%s:%s", bind, port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Printf("FluxDB started on %s", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go fluxdb.HandleConnection(conn)
	}

}

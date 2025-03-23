package fluxdb

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type FluxDB struct {
	data map[string]string
	mu   sync.RWMutex
}

func New() *FluxDB {
	return &FluxDB{
		data: make(map[string]string),
	}
}

// Set - set a key value pair
func (f *FluxDB) Set(key string, value string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return "OK"
}

// Get - get a key value pair
func (f *FluxDB) Get(key string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if val, exists := f.data[key]; exists {
		return val
	}
	return "nil"
}

// Delete - delete a key value pair
func (f *FluxDB) Delete(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return "OK"
}

// Handle connection
func (f *FluxDB) HandleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "PING":
			conn.Write([]byte("PONG\n"))
		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: SET requires KEY VALUE\n"))
				continue
			}
			result := f.Set(parts[1], parts[2])
			conn.Write([]byte(result + "\n"))
		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: GET requires KEY\n"))
				continue
			}
			result := f.Get(parts[1])
			conn.Write([]byte(result + "\n"))
		case "DEL":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: DEL requires KEY\n"))
				continue
			}
			result := f.Delete(parts[1])
			conn.Write([]byte(fmt.Sprintf("%v \n", result)))
		default:
			conn.Write([]byte("ERROR: Unknown command\n"))
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
}

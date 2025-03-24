package fluxdb

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

type FluxDB struct {
	data   map[string]string
	mu     sync.RWMutex
	config map[string]string
}

func New() *FluxDB {

	fluxdb := &FluxDB{
		data:   make(map[string]string),
		config: make(map[string]string),
	}

	fluxdb.config["port"] = "6379"
	fluxdb.config["bind"] = "0.0.0.0"
	fluxdb.config["max_clients"] = "10000"
	fluxdb.config["timeout"] = "0"

	return fluxdb
}

// REDIS OPERATIONS ----------------------------------------------------------------------------------------------------

// Set - set a key value pair
func (f *FluxDB) Set(key string, value string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	// check if key exists
	if _, exists := f.data[key]; exists {
		return "false"
	}

	// set key
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

// REDIS CONFIG OPERATIONS -----------------------------------------------------------------------------------------------

// SetConfig - set a config value
func (f *FluxDB) SetConfig(key string, value string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.config[key] = value
	return "OK"
}

// GetConfig - get a config value
func (f *FluxDB) GetConfig(key string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if val, exists := f.config[key]; exists {
		return val
	}
	return "nil"
}

// RESP PROTOCOL --------------------------------------------------------------------------------------------------------

/**
* RESP Protocol - is used for client-server communication. RESP works on prefixes to indicate the specific data type. [Ends with \r\n]
* The following prefixes are used:
* + Simple Strings - eg: +OK\r\n
* - Errors - eg: -ERR unknown command 'foobar'\r\n
* : Integers - eg: :1000\r\n
* $ Bulk Strings - eg: $6\r\nfoobar\r\n (here, 6 is the length of the string) (if null string is being passed then the length is -1)
* * Arrays - eg: *3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n (here, 3 is the number of elements in the array)
*
* SIMPLE COMMAND IMPLEMENTATION: GET token -> *2\r\n$3\r\nGET\r\n$5\r\n{token}\r\n
* RESP Protocol - https://redis.io/docs/reference/protocol-spec/
 */

func writeString(w io.Writer, s string) {
	fmt.Fprintf(w, "+%s\r\n", s)
}

func writeError(w io.Writer, s string) {
	fmt.Fprintf(w, "-%s\r\n", s)
}

func writeInteger(w io.Writer, i int) {
	fmt.Fprintf(w, ":%d\r\n", i)
}

func writeBulkString(w io.Writer, s string) {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
}

func writeArray(w io.Writer, arr []string) {
	fmt.Fprintf(w, "*%d\r\n", len(arr))
	for _, s := range arr {
		writeBulkString(w, s)
	}
}

// PARSER ---------------------------------------------------------------------------------------------------------------

func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Read the first byte to determine the type
	b, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case '*': // Array
		return parseArray(reader)
	case '$': // Bulk String
		s, err := parseBulkString(reader)
		if err != nil {
			return nil, err
		}
		return []string{s}, nil

	default:
		// For simplicity, try to handle simple strings or inline commands
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		return strings.Fields(string(b) + line), nil
	}
}

// parseArray parses a RESP array
func parseArray(reader *bufio.Reader) ([]string, error) {
	// Read array length
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line)
	}

	if count < 0 {
		return nil, nil // Null array
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		if b != '$' {
			return nil, fmt.Errorf("expected bulk string in array, got: %c", b)
		}

		reader.UnreadByte()
		s, err := parseBulkString(reader)
		if err != nil {
			return nil, err
		}

		result = append(result, s)
	}

	return result, nil
}

// parseBulkString parses a RESP bulk string
func parseBulkString(reader *bufio.Reader) (string, error) {
	// Read the $ character
	_, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// Read length
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = strings.TrimSpace(line)
	length, err := strconv.Atoi(line)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %s", line)
	}

	if length < 0 {
		return "", nil // Null bulk string
	}

	// Read the string content
	buf := make([]byte, length+2) // +2 for \r\n
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return "", err
	}

	return string(buf[:length]), nil
}

// processCommand handles Redis commands
func (r *FluxDB) processCommand(cmd []string, conn net.Conn) {
	if len(cmd) == 0 {
		return
	}

	command := strings.ToUpper(cmd[0])
	args := cmd[1:]

	switch command {
	case "PING":
		if len(args) == 0 {
			writeString(conn, "PONG")
		} else {
			writeBulkString(conn, args[0])
		}

	case "SET":
		if len(args) < 2 {
			writeError(conn, "wrong number of arguments for 'set' command")
			return
		}
		result := r.Set(args[0], args[1])
		writeString(conn, result)

	case "GET":
		if len(args) < 1 {
			writeError(conn, "wrong number of arguments for 'get' command")
			return
		}
		result := r.Get(args[0])
		writeBulkString(conn, result)

	case "DEL":
		if len(args) < 1 {
			writeError(conn, "wrong number of arguments for 'del' command")
			return
		}
		count := 0
		for _, key := range args {
			deleted := r.Delete(key)
			if deleted == "true" {
				count++
			}
		}
		writeInteger(conn, count)

	case "HELP":
		helpText := "Available commands:\r\n" +
			"PING - Test connection\r\n" +
			"SET key value - Set a key value pair\r\n" +
			"GET key - Get a key value pair\r\n" +
			"DEL key - Delete a key value pair\r\n" +
			"CONFIG GET/SET - View or modify configuration\r\n" +
			"SELECT db - Select a logical database\r\n" +
			"HELP - Show this help"

		writeBulkString(conn, helpText)

	case "SELECT":
		if len(args) < 1 {
			writeError(conn, "wrong number of arguments for 'select' command")
			return
		}

		writeString(conn, "OK")

	case "CONFIG":
		if len(args) < 1 {
			writeError(conn, "wrong number of arguments for 'config' command")
			return
		}

		subcommand := strings.ToUpper(args[0])
		switch subcommand {
		case "GET":
			if len(args) < 2 {
				writeError(conn, "wrong number of arguments for 'config get' command")
				return
			}

			pattern := args[1]
			result := []string{}

			// Simple pattern matching (supporting only exact matches and '*')
			if pattern == "*" {
				// Return all config values
				for k, v := range r.config {
					result = append(result, k, v)
				}
			} else {
				// Return specific config value
				if val := r.GetConfig(pattern); val != "" {
					result = append(result, pattern, val)
				}
			}

			writeArray(conn, result)

		case "SET":
			if len(args) < 3 {
				writeError(conn, "wrong number of arguments for 'config set' command")
				return
			}

			r.SetConfig(args[1], args[2])
			writeString(conn, "OK")

		default:
			writeError(conn, "unsupported config operation")
		}

	default:
		writeError(conn, "unknown command '"+command+"'")
	}
}

func (r *FluxDB) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		cmd, err := parseRESP(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error parsing command: %v", err)
			}
			return
		}

		if len(cmd) == 0 {
			continue
		}

		r.processCommand(cmd, conn)
	}
}

package http

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

const (
	responseOKFormat       = "HTTP/1.1 200 OK\r\n\r\n%s"
	responseNOTFOUNDFormat = "HTTP/1.1 404 Not Found\r\n\r\n%s"
	responseErrorFormat    = "HTTP/1.1 500 Internal Server Error\r\n\r\n%s"
)

type request struct {
	requestURI  string
	requestBody []byte
}

type response struct {
	c net.Conn
}

type router struct {
	routes map[string]func(*request, *response)
}

func newRouter() *router {
	return &router{
		routes: make(map[string]func(*request, *response)),
	}
}

type Handler func(...string) string

func (r *router) AddRoute(path string, handler Handler) *router {
	r.routes[path] = func(req *request, rsp *response) {
		args := strings.Split(string(req.requestBody), "|")
		okHandler(req, rsp, handler(args...))
	}
	return r
}

func (r *router) serveHTTP(req *request, rsp *response) {
	handler, ok := r.routes[req.requestURI]
	if ok {
		handler(req, rsp)
	} else {
		notFoundHandler(req, rsp)
	}
}

func StartHttpListen(addr string) *router {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln("Error listening:", err)
	}

	log.Println("Server listening on ", addr)
	router := newRouter()
	go func() {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Error accepting connection:", err)
				continue
			}
			go handleRequest(conn, router)
		}
	}()
	return router
}

func handleRequest(conn net.Conn, router *router) {
	defer conn.Close()
	req := &request{}
	rsp := &response{c: conn}

	reader := bufio.NewReader(conn)
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Error reading request line:", err)
		return
	}

	parts := strings.Fields(requestLine)
	if len(parts) < 3 {
		log.Println("Invalid request format")
		return
	}

	requestMethod := parts[0]
	requestURI := parts[1]
	httpVersion := parts[2]

	log.Printf("Request Method: %s\n", requestMethod)
	log.Printf("Request URI: %s\n", requestURI)
	log.Printf("HTTP Version: %s\n", httpVersion)

	headers := make(map[string]string)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			headerName := strings.TrimSpace(parts[0])
			headerValue := strings.TrimSpace(parts[1])
			headers[headerName] = headerValue
		}
	}

	contentLength, ok := headers["Content-Length"]
	if ok {
		bodySize := 0
		for bodySize < len(contentLength) {
			bodyData := make([]byte, 1024)
			n, err := reader.Read(bodyData)
			if err != nil || n == 0 {
				break
			}

			bodySize += n
			req.requestBody = append(req.requestBody, bodyData[:n]...)
		}
	}
	req.requestURI = requestURI
	router.serveHTTP(req, rsp)
}

func okHandler(req *request, rsp *response, message string) {
	response := fmt.Sprintf(responseOKFormat, message)
	rsp.c.Write([]byte(response))
	rsp.c.Close()
}

func errHandler(req *request, rsp *response, message string) {
	response := fmt.Sprintf(responseErrorFormat, message)
	rsp.c.Write([]byte(response))
	rsp.c.Close()
}

func notFoundHandler(req *request, rsp *response) {
	response := fmt.Sprintf(responseNOTFOUNDFormat, "Page not found")
	rsp.c.Write([]byte(response))
	rsp.c.Close()
}

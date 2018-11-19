package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/rethinkdb/rethinkdb-go.v5/ql2"

	"github.com/jlhawn/reboltdb/json"
	"github.com/jlhawn/reboltdb/query"
	"github.com/jlhawn/reboltdb/server"
)

func main() {
	db, err := bolt.Open(".boltdb", 0666, nil)
	if err != nil {
		log.Fatalf("Unable to open underlying boltdb")
	}
	defer db.Close()

	listener, err := net.Listen("tcp", ":28015")
	if err != nil {
		log.Fatalf("Unable to listen for tcp connections: %s", err)
	}
	defer listener.Close()

	log.Infof("Listening for TCP connections on %s", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Unable to accept connection: %s", err)
		}

		log.Infof("Accepted connection from %s", conn.RemoteAddr())

		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *bolt.DB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	if err := server.DoHandshake(conn, reader); err != nil {
		log.Errorf("Unable to perform handshake: %s", err)
		return
	}

	qs := &queryServer{
		queryCache: map[uint64]struct{}{},
		conn:       conn,
		reader:     reader,
	}

	if err := qs.handleQueries(); err != nil {
		log.Errorf("Unable to handle queries: %s", err)
		return
	}
}

type queryServer struct {
	queryCache map[uint64]struct{}
	conn       net.Conn
	reader     *bufio.Reader
}

func (qs *queryServer) handleQueries() error {
	for {
		// First, read a 64-bit query token.
		var tokenBuf [8]byte
		if _, err := io.ReadFull(qs.reader, tokenBuf[:]); err != nil {
			return fmt.Errorf("unable to read query token into buffer: %s", err)
		}
		token := binary.LittleEndian.Uint64(tokenBuf[:])

		// Next, read the size of the query as a 32-bit integer.
		var sizeBuf [4]byte
		if _, err := io.ReadFull(qs.reader, sizeBuf[:]); err != nil {
			return fmt.Errorf("unable to read query size into buffer: %s", err)
		}
		size := binary.LittleEndian.Uint32(sizeBuf[:])

		queryBuf := make([]byte, int(size))
		if _, err := io.ReadFull(qs.reader, queryBuf); err != nil {
			return fmt.Errorf("unable to read query into buffer: %s", err)
		}

		queryVal, err := json.Parse(queryBuf)
		if err != nil {
			return fmt.Errorf("unable to JSON parse query: %s", err)
		}

		if err := qs.runQuery(token, queryVal); err != nil {
			return fmt.Errorf("unable to handle query: %s", err)
		}

		return nil
	}
}

func (qs *queryServer) runQuery(token uint64, queryVal json.Value) error {
	if !queryVal.IsArray() {
		return fmt.Errorf("expected query type to be array, but found %s", queryVal.ValueType())
	}

	queryArray := queryVal.AsArray()
	if len(queryArray) == 0 || len(queryArray) > 3 {
		return fmt.Errorf("expected 1 to 3 elements in the top-level query, but found %d", len(queryArray))
	}

	if !queryArray[0].IsNumber() {
		return fmt.Errorf("expected query type to be number, but found %s", queryArray[0].ValueType())
	}

	var globalOptArgs json.Object
	if len(queryArray) == 3 {
		if !queryArray[2].IsObject() {
			return fmt.Errorf("expected global optargs to be object, but found %s", queryArray[2].ValueType())
		}
		globalOptArgs = queryArray[2].AsObject()
	}

	queryType := ql2.Query_QueryType(queryArray[0].AsInt64())
	switch queryType {
	case ql2.Query_START:
		if len(queryArray) != 3 {
			return fmt.Errorf("expected 3 elements in top-level START query, but found %d", len(queryArray))
		}

		return qs.startQuery(token, queryArray[1], globalOptArgs)
	case ql2.Query_CONTINUE, ql2.Query_STOP, ql2.Query_NOREPLY_WAIT, ql2.Query_SERVER_INFO:
		return fmt.Errorf("query type %s not yet implemented", ql2.Query_QueryType_name[int32(queryType)])
	default:
		return fmt.Errorf("unrecognized QueryType: %d", queryType)
	}
}

func (qs *queryServer) startQuery(token uint64, value json.Value, globalOptArgs json.Object) error {
	log.Infof("Start Query Global OptArgs: %#v\n", globalOptArgs)

	if _, isDuplicate := qs.queryCache[token]; isDuplicate {
		return fmt.Errorf("duplicate token: %d", token)
	}

	termTree, err := query.MakeTermTree(value)
	if err != nil {
		return fmt.Errorf("unable to parse term tree: %s", err)
	}

	log.Infof("Term Tree:\n%s\n", termTree)

	return nil
}

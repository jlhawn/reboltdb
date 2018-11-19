package server

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"

	"gopkg.in/rethinkdb/rethinkdb-go.v5/ql2"
)

const (
	adminUsername      = "admin"
	adminPasswordSalt  = "6VRzcOVKuS8WWbOKM5Vurw=="
	adminPasswordHash  = "NsWJkSBxXNSiI1Bh0UWM7UXAE3fId5RR1ZnA7Cldtws="
	passwordIterations = 4096
)

func DoHandshake(conn net.Conn, reader *bufio.Reader) error {
	// When we first get a connection, read the magic number for the version of
	// the protobuf targeted by the client (in the [Version] enum). This should
	// **NOT** be sent as a protobuf; it is just sent as a little-endian 32-bit
	// integer over the wire raw. This number should only be sent once per
	// connection.
	var versionBuf [4]byte
	if _, err := io.ReadFull(reader, versionBuf[:]); err != nil {
		return fmt.Errorf("unable to read version magic number into buffer: %s", err)
	}

	// We only support "V1_0".
	version := ql2.VersionDummy_Version(binary.LittleEndian.Uint32(versionBuf[:]))
	if version != ql2.VersionDummy_V1_0 {
		return fmt.Errorf("unrecognized version magic number: %d", version)
	}

	// Reply with a version message.
	if err := writeVersionMessage(conn); err != nil {
		return fmt.Errorf("unable to write version message: %s", err)
	}

	authenticator := &scramAuthenticator{}
	if err := authenticator.readClientAuthenticationMessage(reader); err != nil {
		return fmt.Errorf("unable to read client authentication message: %s", err)
	}

	if err := authenticator.writeServerAuthenticationMessage(conn); err != nil {
		return fmt.Errorf("unable to write server authentication message: %s", err)
	}

	if err := authenticator.readClientAuthenticationProof(reader); err != nil {
		return fmt.Errorf("unable to read client authentication proof: %s", err)
	}

	if err := authenticator.writeServerAuthenticationSignatureMessage(conn); err != nil {
		return fmt.Errorf("unable to write server authentication signature: %s", err)
	}

	return nil
}

type versionMessage struct {
	Success            bool   `json:"success"`
	MinProtocolVersion int    `json:"min_protocol_version"`
	MaxProtocolVersion int    `json:"max_protocol_version"`
	ServerVersion      string `json:"server_version"`
}

func writeVersionMessage(conn net.Conn) error {
	versionResponseBuf, err := json.Marshal(versionMessage{
		Success:            true,
		MinProtocolVersion: 0,
		MaxProtocolVersion: 0,
		ServerVersion:      "ReboltDB 0.1.0",
	})
	if err != nil {
		return fmt.Errorf("unable to JSON encode version response: %s", err)
	}
	versionResponseBuf = append(versionResponseBuf, '\x00')
	n, err := conn.Write(versionResponseBuf)
	if err != nil {
		return err
	}
	if n != len(versionResponseBuf) {
		return io.ErrShortWrite
	}

	return nil
}

type scramAuthenticator struct {
	authMessage     string
	clientNonce     string
	serverNonce     string
	serverSignature string
}

type clientAuthenticationMessage struct {
	ProtocolVersion      int    `json:"protocol_version"`
	Authentication       string `json:"authentication"`
	AuthenticationMethod string `json:"authentication_method"`
}

func (a *scramAuthenticator) readClientAuthenticationMessage(reader *bufio.Reader) error {
	// Next, the client will send a JSON payload followed by null character.
	buf, err := reader.ReadBytes('\x00')
	if err != nil {
		return err
	}
	// Strip  null byte.
	buf = buf[:len(buf)-1]

	var message clientAuthenticationMessage
	if err := json.Unmarshal(buf, &message); err != nil {
		return fmt.Errorf("unable to JSON decode client authentication message: %s", err)
	}

	if message.ProtocolVersion != 0 {
		return fmt.Errorf("unrecognized protocol version: %d", message.ProtocolVersion)
	}

	if message.AuthenticationMethod != "SCRAM-SHA-256" {
		return fmt.Errorf("unrecognized authentication method: %s", message.AuthenticationMethod)
	}

	if !strings.HasPrefix(message.Authentication, "n,,") {
		return fmt.Errorf("invalid authentication encoding")
	}

	a.authMessage = strings.TrimPrefix(message.Authentication, "n,,")
	attrs := strings.Split(a.authMessage, ",")
	for _, attr := range attrs {
		if pair := strings.SplitN(attr, "=", 2); len(pair) == 2 {
			switch pair[0] {
			case "n":
				if pair[1] != adminUsername {
					return fmt.Errorf("username must be %q", adminUsername)
				}
			case "r":
				a.clientNonce = pair[1]
			default:
				return fmt.Errorf("invalid authentication attribute key: %q", pair[0])
			}
		} else {
			return fmt.Errorf("invalid authentication attribute: %q", attr)
		}
	}

	return nil
}

type serverAuthenticationMessage struct {
	Success        bool   `json:"success"`
	Authentication string `json:"authentication"`
}

func (a *scramAuthenticator) writeServerAuthenticationMessage(conn net.Conn) error {
	var serverNonceBuf [18]byte
	if _, err := io.ReadFull(rand.Reader, serverNonceBuf[:]); err != nil {
		return fmt.Errorf("unable to generate random server nonce: %s", err)
	}
	a.serverNonce = a.clientNonce + base64.StdEncoding.EncodeToString(serverNonceBuf[:])

	attributes := []string{
		fmt.Sprintf("r=%s", a.serverNonce),
		fmt.Sprintf("s=%s", adminPasswordSalt),
		fmt.Sprintf("i=%d", passwordIterations),
	}

	message := serverAuthenticationMessage{
		Success:        true,
		Authentication: strings.Join(attributes, ","),
	}

	a.authMessage += "," + message.Authentication

	payloadBuf, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("unable to JSON encode server authentication message: %s", err)
	}
	payloadBuf = append(payloadBuf, '\x00')
	n, err := conn.Write(payloadBuf)
	if err != nil {
		return err
	}
	if n != len(payloadBuf) {
		return io.ErrShortWrite
	}

	return nil
}

func (a *scramAuthenticator) readClientAuthenticationProof(reader *bufio.Reader) error {
	// Next, the client will send another JSON payload followed by a null
	// character.
	buf, err := reader.ReadBytes('\x00')
	if err != nil {
		return err
	}
	// Strip  null byte.
	buf = buf[:len(buf)-1]

	var message clientAuthenticationMessage
	if err := json.Unmarshal(buf, &message); err != nil {
		return fmt.Errorf("unable to JSON decode client authentication proof message: %s", err)
	}

	// Calculate the client proof.
	decodedPasswordHash, err := base64.StdEncoding.DecodeString(adminPasswordHash)
	if err != nil {
		return fmt.Errorf("unable to decode stored password hash: %s", err)
	}
	mac := hmac.New(sha256.New, decodedPasswordHash)
	mac.Write([]byte("Client Key"))
	clientKey := mac.Sum(nil)

	storedKey := sha256.Sum256(clientKey)

	a.authMessage += "," + message.Authentication[:strings.Index(message.Authentication, ",p=")]

	mac = hmac.New(sha256.New, storedKey[:])
	mac.Write([]byte(a.authMessage))
	clientSignature := mac.Sum(nil)

	clientProofBuf := make([]byte, len(clientKey))
	for i := range clientProofBuf {
		clientProofBuf[i] = clientKey[i] ^ clientSignature[i]
	}
	clientProof := base64.StdEncoding.EncodeToString(clientProofBuf)

	if !strings.HasPrefix(message.Authentication, "c=biws,") {
		return fmt.Errorf("invalid authentication encoding")
	}
	encodedAttributes := strings.TrimPrefix(message.Authentication, "c=biws,")

	var validNonce, validProof bool
	attrs := strings.Split(encodedAttributes, ",")
	for _, attr := range attrs {
		if pair := strings.SplitN(attr, "=", 2); len(pair) == 2 {
			switch pair[0] {
			case "r":
				if pair[1] != a.serverNonce {
					return fmt.Errorf("invalid server nonce: got %q, expected %q", pair[1], a.serverNonce)
				}
				validNonce = true
			case "p":
				if pair[1] != clientProof {
					return fmt.Errorf("invalid client proof: got %q, expected %q", pair[1], clientProof)
				}
				validProof = true
			default:
				return fmt.Errorf("invalid authentication attribute key: %q", pair[0])
			}
		} else {
			return fmt.Errorf("invalid authentication attribute: %q", attr)
		}
	}
	if !(validNonce && validProof) {
		return fmt.Errorf("invalid authentication attributes")
	}

	// Create the server signature.
	mac = hmac.New(sha256.New, decodedPasswordHash)
	mac.Write([]byte("Server Key"))
	serverKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, serverKey)
	mac.Write([]byte(a.authMessage))
	a.serverSignature = base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return nil
}

func (a *scramAuthenticator) writeServerAuthenticationSignatureMessage(conn net.Conn) error {
	payloadBuf, err := json.Marshal(serverAuthenticationMessage{
		Success:        true,
		Authentication: fmt.Sprintf("v=%s", a.serverSignature),
	})
	if err != nil {
		return fmt.Errorf("unable to JSON encode server authentication message: %s", err)
	}
	payloadBuf = append(payloadBuf, '\x00')
	n, err := conn.Write(payloadBuf)
	if err != nil {
		return err
	}
	if n != len(payloadBuf) {
		return io.ErrShortWrite
	}

	return nil
}

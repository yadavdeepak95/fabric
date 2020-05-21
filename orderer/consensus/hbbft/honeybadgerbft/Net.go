package honeybadgerbft

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"fmt"
// 	"net"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/golang/protobuf/proto"
// 	ab "github.com/hyperledger/fabric/protos/orderer"
// 	"github.com/hyperledger/fabric/protos/utils"
// )

// var connectedOrderers = make(map[string]*net.TCPConn)
// var registedChains = make(map[string]MessageChannels)
// var listener *net.TCPListener
// var sendChannels = make(map[string]chan ab.HoneyBadgerBFTMessage)
// var accepted = make(chan interface{})
// var lock sync.Mutex

// type MessageChannels struct {
// 	Send    chan ab.HoneyBadgerBFTMessage
// 	Receive chan *ab.HoneyBadgerBFTMessage
// }

// func Register(chainID string, connectAddresses []string, selfIndex int) (MessageChannels, error) {
// 	if result, exist := registedChains[chainID]; exist {
// 		return result, nil
// 	}
// 	channels := MessageChannels{
// 		Send:    make(chan ab.HoneyBadgerBFTMessage),
// 		Receive: make(chan *ab.HoneyBadgerBFTMessage),
// 	}
// 	//create channel for chainID
// 	registedChains[chainID] = channels

// 	selfAddress := strings.TrimSpace(connectAddresses[selfIndex])

// 	//This listener package listen only on one port... 1 orderer == 1 port
// 	if listener == nil {
// 		//opening port for listening
// 		if err := listen(selfAddress); err != nil {
// 			return MessageChannels{}, err
// 		}
// 		//listen service
// 		go acceptConnectionService()
// 	} else {
// 		if listener.Addr().String() != selfAddress {
// 			return MessageChannels{}, fmt.Errorf("Already listen %s, can not listen %s", listener.Addr().String(), selfAddress)
// 		} else {
// 			logger.Debugf("Reuse listener binding %s", selfAddress)
// 		}
// 	}

// 	//dispatch channel for each addr and not for each channel
// 	//TODO MAKE IT FOR EACH CHANNEL future may help may not.
// 	for _, addr := range connectAddresses {
// 		sendChannels[addr] = make(chan ab.HoneyBadgerBFTMessage, 666666)
// 		go sendByChannel(sendChannels[addr], connectAddresses, addr)
// 	}

// 	// //dial
// 	// dial(connectAddresses)
// 	// //send service
// 	go sendMessageService(channels.Send, connectAddresses, chainID, selfIndex)
// 	// //
// 	// for range connectAddresses {
// 	// 	<-accepted
// 	// }

// 	return channels, nil
// }

// func dial(addresses []string) {
// 	finished := make(chan bool)
// 	dialOneFunc := func(address string) {
// 		address = strings.TrimSpace(address)
// 		lock.Lock()
// 		_, exist := connectedOrderers[address]
// 		lock.Unlock()
// 		if exist {
// 			return
// 		}
// 		tcpaddr, err := net.ResolveTCPAddr("tcp", address)
// 		if err != nil {
// 			logger.Panic("Can not resolve address: ", address)
// 		}
// 		var tried int
// 		var tcpconn *net.TCPConn
// 		for { //TODO: 换一个更好的重试逻辑
// 			tcpconn, err = net.DialTCP("tcp4", nil, tcpaddr)
// 			// err := nil
// 			if err == nil {
// 				break
// 			}
// 			tried++
// 			// logger.Debugf("Can not connect %s: %s", address, err)
// 			time.Sleep(2 * time.Second) //TODO: 避免硬编码等待时间
// 		}
// 		logger.Debugf("Connected to %s", address)
// 		lock.Lock()
// 		connectedOrderers[address] = tcpconn
// 		lock.Unlock()
// 		finished <- true
// 		return
// 	}
// 	for _, addr := range addresses {
// 		go dialOneFunc(addr)
// 	}
// 	for range addresses {
// 		<-finished
// 	}
// }

// func sendMessageService(channel chan ab.HoneyBadgerBFTMessage, address []string, chainID string, selfIndex int) {
// 	for {
// 		msg := <-channel
// 		// logger.Debugf("TO SEND %+v", msg)
// 		receiver := msg.GetReceiver()
// 		// conn := connectedOrderers[address[receiver]]
// 		msg.Sender = uint64(selfIndex)
// 		msg.ChainId = chainID
// 		channel, ok := sendChannels[address[receiver]]

// 		if !ok {
// 			logger.Debugf("Channel not found for the address,%v,chain ID : %v", receiver, chainID)
// 		}
// 		if cap(channel) > len(channel) {
// 			// logger.Debugf("%+v", msg)
// 			channel <- msg
// 		} else {
// 			//TODO THROW WARNING
// 		}

// 	}
// }

// func sendByChannel(channel chan ab.HoneyBadgerBFTMessage, address []string, receiver string) {
// 	for msg := range channel {
// 		// logger.Debugf("GOT MSG TO SEND %+v", msg)
// 		data := utils.MarshalOrPanic(&msg)
// 		buf := bytes.NewBuffer(convertInt32ToBytes(int32(len(data))))
// 		buf.Write(data)
// 		lock.Lock()
// 		conn, ok := connectedOrderers[receiver]
// 		lock.Unlock()
// 		if !ok {
// 			dial([]string{receiver})
// 			lock.Lock()
// 			conn = connectedOrderers[receiver]
// 			lock.Unlock()
// 		}
// 		// logger.Debugf("GOT CONNECTION")
// 		_, err := conn.Write(buf.Bytes()) //TODO: 加锁sync.Mutex
// 		//TODO check if conn is broken or there is no conection
// 		if err != nil {
// 			logger.Debugf("Send message to %s failed: %s", receiver, err)
// 		}
// 		// logger.Debugf("SENT")
// 	}
// }
// func convertInt32ToBytes(value int32) []byte {
// 	bytesBuffer := bytes.NewBuffer([]byte{})
// 	binary.Write(bytesBuffer, binary.BigEndian, value)
// 	return bytesBuffer.Bytes()
// } // logger.Debugf("Sending message to:%v , From : %v", receiver, msg.Sender)

// func listen(address string) error {
// 	tcpaddr, err := net.ResolveTCPAddr("tcp4", address)
// 	if err != nil {
// 		return err
// 	}
// 	tcplisten, err := net.ListenTCP("tcp4", tcpaddr)
// 	if err != nil {
// 		return err
// 	}
// 	listener = tcplisten
// 	logger.Infof("HoneyBadgerBFT Service listen at %s", address)
// 	return nil
// }

// func acceptConnectionService() {
// 	logger.Debugf("Start accept connection at %s", listener.Addr())
// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			logger.Errorf("Error occured when accepting an connection: %s", err)
// 			continue
// 		}
// 		logger.Debugf("Accepted a connection from %s", conn.RemoteAddr())
// 		//TODO : CHECK VALIDITY OF CONNECTION
// 		go readConnectionService(conn)

// 	}
// }

// func readConnectionService(connection net.Conn) {
// 	logger.Debug("in readconnection")
// 	defer connection.Close()
// 	for {
// 		var buf = make([]byte, 4)
// 		length, err := connection.Read(buf)
// 		if err != nil {
// 			logger.Warningf("Error occured when reading message length from %s: %s", connection.RemoteAddr(), err)
// 			return
// 		}
// 		if length != 4 {
// 			logger.Warningf("Can not read full message length bytes %s", connection.RemoteAddr())
// 		}
// 		msgLength, err := convertBytesToInt32(buf)
// 		if err != nil {
// 			logger.Warningf("Error occured when converting message length from bytes %s: %s", connection.RemoteAddr(), err)
// 		}
// 		msg := bytes.NewBuffer([]byte{})
// 		for msgLength > 0 {
// 			msgBuf := make([]byte, msgLength)
// 			length, err = connection.Read(msgBuf)
// 			if err != nil {
// 				logger.Warningf("Error occured when reading from %s: %s", connection.RemoteAddr(), err)
// 			}
// 			if length != int(msgLength) {
// 				logger.Warningf("Can not read full message from %s: require %v - received %v", connection.RemoteAddr(), msgLength, length)
// 			}
// 			msg.Write(msgBuf[:length])
// 			msgLength -= int32(length)
// 		}
// 		processReceivedData(msg.Bytes())
// 	}
// }

// func convertBytesToInt32(data []byte) (result int32, err error) {
// 	err = binary.Read(bytes.NewBuffer(data), binary.BigEndian, &result)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return result, nil
// }

// func processReceivedData(data []byte) error {
// 	var msg = new(ab.HoneyBadgerBFTMessage)
// 	if err := proto.Unmarshal(data, msg); err != nil {
// 		return err
// 	}
// 	chainID := msg.GetChainId()
// 	channels, exist := registedChains[chainID]
// 	if !exist {
// 		return fmt.Errorf("ChainID (%s) in received message not registered", chainID)
// 	}
// 	channels.Receive <- msg
// 	return nil
// }

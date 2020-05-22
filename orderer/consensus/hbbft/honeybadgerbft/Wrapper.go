package honeybadgerbft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	ab "github.com/hyperledger/fabric/orderer/consensus/hbbft/honeybadgerbft/proto/orderer"
	cb "github.com/hyperledger/fabric/protos/common"

	"github.com/op/go-logging"
)

const pkgLogID = "orderer/HoneybadgerBFT"

var logger = logging.MustGetLogger(pkgLogID)

// MessageChannels to send and recive from network
type MessageChannels struct {
	Send    chan ab.HoneyBadgerBFTMessage
	Receive chan *ab.HoneyBadgerBFTMessage
	Outtxn  chan *cb.Envelope
}

// ChainImpl struct
type ChainImpl struct {
	Total         int
	Tolerance     int
	Index         int
	height        uint64
	batchSize     uint64
	txbuffer      []*cb.Envelope
	heightChan    map[uint64](chan *ab.HoneyBadgerBFTMessage)
	newBlockChan  chan interface{}
	bufferLock    sync.Mutex
	replayChan    chan interface{}
	heightMapLock sync.Mutex
	heightLock    sync.Mutex
	// blockOut      chan interface{}
	MsgChan   *MessageChannels
	tempCount uint64
}

//used to maintain to call for next proposal
// var tempCount uint64

//Enqueue txn input and trigger for new block
func (ch *ChainImpl) Enqueue(env *cb.Envelope, isConfig bool) {
	ch.bufferLock.Lock()
	ch.tempCount++
	ch.txbuffer = append(ch.txbuffer, env)
	fmt.Printf("\nGOT txn[%v]batchsize[%v] tempcount[%v]\n\n", ch.Index, ch.batchSize, ch.tempCount)

	if (ch.tempCount >= ch.batchSize) || isConfig {
		fmt.Printf("\n Triggering BLOCK CHAIN [%v] cout[%v] config[%v]", ch.Index, ch.tempCount, isConfig)
		ch.tempCount = 0
		ch.newBlockChan <- nil
	}

	ch.bufferLock.Unlock()
}

//NewWrapper create new insatance of chain
func NewWrapper(batchSize int, Index int, total int) *ChainImpl {
	//Net Package opening port and returns channels
	//TODO : add chain id here
	//  if req
	logging.SetLevel(4, pkgLogID)
	// logger.Infof("inFO LEVEL SET")
	// messageChannels, err := Register("1", ConnectionList, Index)

	channels := &MessageChannels{
		Send:    make(chan ab.HoneyBadgerBFTMessage, 66666),
		Receive: make(chan *ab.HoneyBadgerBFTMessage, 66666),
		Outtxn:  make(chan *cb.Envelope, 66666),
	}

	ch := &ChainImpl{
		batchSize:    uint64(1), //,uint64(batchSize),
		Total:        total,
		Tolerance:    ((total - 1) / 3),
		Index:        Index,
		MsgChan:      channels,
		height:       0,
		newBlockChan: make(chan interface{}, 666666),
		// blockOut:     make(chan interface{}),
		replayChan: make(chan interface{}, 666666),
		heightChan: make(map[uint64]chan *ab.HoneyBadgerBFTMessage),
		tempCount:  0,
	}

	fmt.Printf("WRAPPER FIRED UP***********************8")
	return ch

}

//Start fires to go routines
func (ch *ChainImpl) Start() {
	//Send msg to channel according to height
	// go ch.run()
	//main function does all main stuff propose/consensus and all
	go ch.Consensus()
	// mapping message to proper channel
	dispatchMessageByHeightService := func() {
		for {
			msg := <-ch.MsgChan.Receive
			// fmt.Printf("RECVED MSGS**********************\n\n%v\n\n", msg)
			ch.heightLock.Lock()
			if msg.GetHeight() >= ch.height {
				ch.getByHeight(msg.GetHeight()) <- msg
			}
			ch.heightLock.Unlock()
		}
	}
	go dispatchMessageByHeightService()
}

//maintain map of channels according to height
func (ch *ChainImpl) getByHeight(height uint64) chan *ab.HoneyBadgerBFTMessage {
	ch.heightMapLock.Lock()
	defer ch.heightMapLock.Unlock()
	result, exist := ch.heightChan[height]
	if !exist {
		//TODO: calculate height according to number of nodes
		result = make(chan *ab.HoneyBadgerBFTMessage, 6666)
		ch.heightChan[height] = result
	}

	return result
}

// func (ch *ChainImpl) run() {
// 	//if hb message send to HB it will handle according to height
// 	//DONE if tx in check for no of txn overbatch with counter if count excced call block cutting --DONE
// 	//DONE if required block out can also trigger new block (if there is no block in and no of txn are more then block limit it should trigger)
// 	for {
// 		msg := <-ch.MsgChan.Receive
// 		ch.replayChan <- msg
// 	}

// }

func unique(intSlice []*cb.Envelope) []*cb.Envelope {
	keys := make(map[string]bool)
	list := []*cb.Envelope{}
	for _, entry := range intSlice {
		if _, value := keys[entry.String()]; !value {
			keys[entry.String()] = true
			list = append(list, entry)
		}
	}
	return list
}

//Consensus main
func (ch *ChainImpl) Consensus() {
	for {

		sendFunc := func(index int, msg ab.HoneyBadgerBFTMessage) {
			msg.Height = ch.height
			msg.Receiver = uint64(index)
			msg.Sender = uint64(ch.Index)
			// fmt.Printf("SENDING MSGS**********************\n\n%+v\n\n", msg)
			ch.MsgChan.Send <- msg
		}
		broadcastFunc := func(msg ab.HoneyBadgerBFTMessage) {
			for i := 0; i < ch.Total; i++ {
				sendFunc(i, msg)
			}
		}
		select {
		// case <-ch.exitChan:
		// 	logger.Debug("Exiting")
		// 	return
		case <-ch.newBlockChan:
			var exitHeight = make(chan interface{})
			ch.heightLock.Lock()
			logger.Infof("\n\n***************%v[%v]*****************\n\n", ch.height, ch.Index)
			ch.heightLock.Unlock()
			startTime := time.Now()
			var abaRecvMsgChannel = make(chan *ab.HoneyBadgerBFTMessage)
			var rbcRecvMsgChannel = make(chan *ab.HoneyBadgerBFTMessage)

			///////////////////////////////
			//Distribute msg by serivce
			//////////////////////////////
			dispatchByTypeService := func() {
				for {
					select {
					case <-exitHeight:
						return
					case msg := <-ch.getByHeight(ch.height):

						if msg.GetBinaryAgreement() != nil {
							abaRecvMsgChannel <- msg
						} else if msg.GetReliableBroadcast() != nil {
							rbcRecvMsgChannel <- msg
						} else {
							logger.Debugf("MSG in Serive")
						}

					}
				}
			}
			go dispatchByTypeService()

			//////////////////////////////
			//channel for every instance
			//////////////////////////////
			var abaInstanceRecvMsgChannels = make([]chan *ab.HoneyBadgerBFTMessage, ch.Total)
			var rbcInstanceRecvMsgChannels = make([]chan *ab.HoneyBadgerBFTMessage, ch.Total)

			for i := 0; i < ch.Total; i++ {
				abaInstanceRecvMsgChannels[i] = make(chan *ab.HoneyBadgerBFTMessage, 666666)
				rbcInstanceRecvMsgChannels[i] = make(chan *ab.HoneyBadgerBFTMessage, 666666)
			}

			dispatchByInstance := func() {
				for {
					select {
					case <-exitHeight:
						return
					case msg := <-abaRecvMsgChannel:
						abaInstanceRecvMsgChannels[msg.GetInstance()] <- msg
					case msg := <-rbcRecvMsgChannel:
						rbcInstanceRecvMsgChannels[msg.GetInstance()] <- msg
					}
				}
			}
			go dispatchByInstance()

			//////////////////////////
			//setting up RBC ABA
			//////////////////////////
			var aba = make([]*BinaryAgreement, ch.Total)
			var rbc = make([]*ReliableBroadcast, ch.Total)
			for i := 0; i < ch.Total; i++ {
				instanceIndex := i // NOTE important to copy i
				componentSendFunc := func(index int, msg ab.HoneyBadgerBFTMessage) {

					msg.Instance = uint64(instanceIndex)
					logger.Debugf("msg.Instance[%v],orderer[%v]", msg.Instance, ch.Index, instanceIndex)
					sendFunc(index, msg)
				}
				componentBroadcastFunc := func(msg ab.HoneyBadgerBFTMessage) {
					msg.Instance = uint64(instanceIndex)
					broadcastFunc(msg)
				}
				rbc[i] = NewReliableBroadcast(i, ch.Total, ch.Tolerance, ch.Index, i, rbcInstanceRecvMsgChannels[i], componentSendFunc, componentBroadcastFunc) // TODO: a better way to eval whether i is leader, ListenAddress may difference with address in connectionlist
				aba[i] = NewBinaryAgreement(i, ch.Total, ch.Tolerance, abaInstanceRecvMsgChannels[i], componentBroadcastFunc)                                   // May stop automatically?				                                                                                                                                                                    //TODO
			}

			///////////////////////////////////////
			//setup ACS component (using N instances of COIN ABA RBC)
			///////////////////////////////////////
			acs := NewCommonSubset(ch.Index, ch.Total, ch.Tolerance, rbc, aba)

			////////////////////////////////////////////////
			//setup HoneyBadgerBFT component (using ACS)
			////////////////////////////////////////////////
			block := NewHoneyBadgerBlock(ch.Total, ch.Tolerance, acs)
			//TODO : Improve ??
			randomSelectFunc := func(batch []*cb.Envelope, number int) (result []*cb.Envelope) {
				result = batch[:]
				if len(batch) <= number {
					return result
				}
				for len(result) > number {
					i := rand.Intn(len(batch))
					result = append(result[:i], result[i+1:]...)
				}
				return result
			}

			ch.bufferLock.Lock()
			proposalBatch := ch.txbuffer[:]
			ch.bufferLock.Unlock()
			if uint32(len(proposalBatch)) > uint32(ch.batchSize) {
				ch.bufferLock.Lock()
				proposalBatch = ch.txbuffer[:ch.batchSize]
				ch.bufferLock.Unlock()

			}
			proposalBatch = randomSelectFunc(proposalBatch, int(ch.batchSize))

			////////////////////////////////////////////
			//generate blocks
			///////////////////////////////////////////

			var committedBatch []*cb.Envelope

			block.In <- proposalBatch
			committedBatch = <-block.Out

			// <-ch.blockOut

			//TODO MAKE IT BETTER Clear buffer
			if len(committedBatch) == 0 {
				logger.Warningf("No transaction committed!")
			} else {
				//removing duplicate txns
				lenbefore := len(committedBatch)
				committedBatch = unique(committedBatch)
				fmt.Printf("\n\nAFTER DELETING DUPLICATE***\nB=%v A=%v\n\n", lenbefore, len(committedBatch))
				ch.bufferLock.Lock()

				temp := []*cb.Envelope{}
				for _, v := range ch.txbuffer {
					in := true
					for _, tx := range committedBatch {
						if v.String() == tx.String() {
							in = false
							break
						}
					}
					if in {
						temp = append(temp, v)
					}
				}
				ch.txbuffer = temp
				ch.bufferLock.Unlock()
				for _, tx := range committedBatch {
					ch.MsgChan.Outtxn <- tx
				}
			}

			ch.heightLock.Lock()
			logger.Infof("%v, %v, %v", ch.height, len(committedBatch), time.Since(startTime))

			logger.Infof("\n\n********************Bufferlen == %v**************\n\n", len(ch.txbuffer))
			delete(ch.heightChan, ch.height)
			ch.height++
			ch.heightLock.Unlock()
			close(exitHeight)
			//Remove from
			//CLEANING
			for i := 0; i < ch.Total; i++ {
				close(rbc[i].exitRecv)
				close(aba[i].exitRecv)
			}
			logger.Infof("ch.blockout[%v]", ch.Index)
			if len(ch.newBlockChan) == 0 {

				ch.bufferLock.Lock()
				if uint64(len(ch.txbuffer)) >= ch.batchSize {
					logger.Debugf("OUT PUSHING HEIGHT")
					ch.newBlockChan <- nil
				}
				ch.bufferLock.Unlock()
			}
		}

	}
}

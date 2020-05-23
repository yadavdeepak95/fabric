package honeybadgerbft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	ab "github.com/hyperledger/fabric/orderer/consensus/hbbft/honeybadgerbft/proto/orderer"
	cb "github.com/hyperledger/fabric/protos/common"
	// 	"github.com/op/go-logging"
)

const pkgLogID = "orderer/HoneybadgerBFT"

// var logger *flogging.FabricLogger

// MessageChannels to send and recive from network or rpc
type MessageChannels struct {
	Send    chan ab.HoneyBadgerBFTMessage
	Receive chan *ab.HoneyBadgerBFTMessage
	Outtxn  chan *cb.Envelope
}

// ChainImpl struct
type ChainImpl struct {
	Total         int // total partcipating nodes
	Tolerance     int
	Index         int
	height        uint64 // internal use
	BatchSize     uint64
	channelName   string
	txbuffer      []*cb.Envelope
	heightChan    map[uint64](chan *ab.HoneyBadgerBFTMessage)
	newBlockChan  chan interface{}
	bufferLock    sync.Mutex
	heightMapLock sync.Mutex
	heightLock    sync.Mutex
	MsgChan       *MessageChannels
	tempCount     uint64
	BatchTimeOut  time.Duration
	timer         <-chan time.Time
	logger        *flogging.FabricLogger
	exitChan      chan interface{}
}

//used to maintain to call for next proposal
// var tempCount uint64

//Enqueue txn input and trigger for new block
func (ch *ChainImpl) Enqueue(env *cb.Envelope, isConfig bool) {

	ch.bufferLock.Lock()
	ch.tempCount++
	ch.txbuffer = append(ch.txbuffer, env)
	fmt.Printf("\nGOT txn[%v]batchsize[%v] tempcount[%v]\n\n", ch.Index, ch.BatchSize, ch.tempCount)
	ch.timer = time.After(ch.BatchTimeOut)
	if (ch.tempCount >= ch.BatchSize) || isConfig {
		fmt.Printf("\n Triggering BLOCK CHAIN [%v] cout[%v] config[%v]", ch.Index, ch.tempCount, isConfig)
		ch.tempCount = 0
		ch.newBlockChan <- nil
	}

	ch.bufferLock.Unlock()
}

//NewWrapper create new insatance of chain
func NewWrapper(batchSize int, Index int, total int, BatchTimeOut time.Duration, channnelName string, logger *flogging.FabricLogger) *ChainImpl {

	// logging.SetLevel(4, pkgLogID)
	// logger.Infof("inFO LEVEL SET")
	// messageChannels, err := Register("1", ConnectionList, Index)

	channels := &MessageChannels{
		Send:    make(chan ab.HoneyBadgerBFTMessage, 66666),
		Receive: make(chan *ab.HoneyBadgerBFTMessage, 66666),
		Outtxn:  make(chan *cb.Envelope, 66666),
	}

	ch := &ChainImpl{
		channelName:  channnelName,
		BatchSize:    uint64(batchSize),
		BatchTimeOut: BatchTimeOut / 2,
		Total:        total,
		Tolerance:    ((total - 1) / 3),
		Index:        Index,
		MsgChan:      channels,
		height:       0,
		newBlockChan: make(chan interface{}, 666666),
		heightChan:   make(map[uint64]chan *ab.HoneyBadgerBFTMessage),
		tempCount:    0,
		logger:       logger,
	}
	// logger = ch.logger

	ch.logger.Infof("honeybadger Initiated channel[%v]", ch.channelName)
	return ch

}

//Start fires to go routines
func (ch *ChainImpl) Start() {
	// main functions
	go ch.Consensus()
	// mapping incomming messages to proper channel
	dispatchMessageByHeightService := func() {
		for {
			select {
			case msg := <-ch.MsgChan.Receive:
				ch.heightLock.Lock()
				if msg.GetHeight() >= ch.height {
					ch.getByHeight(msg.GetHeight()) <- msg
				}
				ch.heightLock.Unlock()

			case <-ch.exitChan:
				ch.logger.Info("Exiting honeybadger dispatchMessageByHeightService loop")
				return
			}
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
		result = make(chan *ab.HoneyBadgerBFTMessage, 66666)
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
// delete duplicate txn in one batch
func unique(txnSlice []*cb.Envelope) []*cb.Envelope {
	keys := make(map[string]bool)
	list := []*cb.Envelope{}
	for _, entry := range txnSlice {
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
			ch.MsgChan.Send <- msg
		}
		broadcastFunc := func(msg ab.HoneyBadgerBFTMessage) {
			for i := 0; i < ch.Total; i++ {
				sendFunc(i, msg)
			}
		}

		select {
		case <-ch.exitChan:
			ch.logger.Info("Exiting honeybadger main loop channel[%v]", ch.channelName)
			return
		case <-ch.newBlockChan:
			// handle extra triggers
			ch.timer = nil
			ch.bufferLock.Lock()
			if len(ch.txbuffer) <= 0 {
				ch.timer = nil
				ch.bufferLock.Unlock()
				continue
			}
			ch.bufferLock.Unlock()
			var exitHeight = make(chan interface{})
			ch.heightLock.Lock()
			ch.logger.Infof("Node[%v], Channel[%v], Height[%v]", ch.height, ch.channelName, ch.Index)
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
							ch.logger.Warning("Invalid Msg")
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
					ch.logger.Debugf("msg.Instance[%v],orderer[%v]", msg.Instance, ch.Index, instanceIndex)
					sendFunc(index, msg)
				}
				componentBroadcastFunc := func(msg ab.HoneyBadgerBFTMessage) {
					msg.Instance = uint64(instanceIndex)
					broadcastFunc(msg)
				}
				rbc[i] = NewReliableBroadcast(i, ch.Total, ch.Tolerance, ch.Index, i, rbcInstanceRecvMsgChannels[i], componentSendFunc, componentBroadcastFunc, ch.logger) // TODO: a better way to eval whether i is leader, ListenAddress may difference with address in connectionlist
				aba[i] = NewBinaryAgreement(i, ch.Total, ch.Tolerance, abaInstanceRecvMsgChannels[i], componentBroadcastFunc, ch.logger)                                   // May stop automatically?				                                                                                                                                                                    //TODO
			}

			///////////////////////////////////////
			//setup ACS component (using N instances of COIN ABA RBC)
			///////////////////////////////////////
			acs := NewCommonSubset(ch.Index, ch.Total, ch.Tolerance, rbc, aba, ch.logger)

			////////////////////////////////////////////////
			//setup HoneyBadgerBFT component (using ACS)
			////////////////////////////////////////////////
			block := NewHoneyBadgerBlock(ch.Total, ch.Tolerance, acs, ch.logger)

			//TODO : Improve --- I guess is working but need to check properly
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
			if uint32(len(proposalBatch)) > uint32(ch.BatchSize) {
				// ch.bufferLock.Lock()
				proposalBatch = proposalBatch[:ch.BatchSize]
				// ch.bufferLock.Unlock()
				proposalBatch = randomSelectFunc(proposalBatch, int(ch.BatchSize))
			}

			////////////////////////////////////////////
			//generate blocks
			///////////////////////////////////////////

			var committedBatch []*cb.Envelope

			block.In <- proposalBatch
			committedBatch = <-block.Out

			//TODO MAKE IT BETTER Clear buffer
			if len(committedBatch) == 0 {
				ch.logger.Warningf("No transaction committed!")
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
			ch.logger.Infof("Block out at height[%v] txns[%v] timetaken[%v] buffer left[%v]", ch.height, len(committedBatch), time.Since(startTime), len(ch.txbuffer))
			delete(ch.heightChan, ch.height)
			ch.height++
			ch.heightLock.Unlock()

			//CLEANING
			close(exitHeight)
			for i := 0; i < ch.Total; i++ {
				close(rbc[i].exitRecv)
				close(aba[i].exitRecv)
			}
			if len(ch.newBlockChan) == 0 {

				ch.bufferLock.Lock()
				if uint64(len(ch.txbuffer)) >= ch.BatchSize {
					ch.logger.Debugf("OUT PUSHING HEIGHT")
					ch.newBlockChan <- nil
				} else {
					ch.timer = time.After(ch.BatchTimeOut)
				}
				ch.bufferLock.Unlock()
			}
		case <-ch.timer:
			ch.logger.Infof("Trigering to honeybadger to do consensus")
			ch.bufferLock.Lock()
			if len(ch.txbuffer) > 0 {
				ch.newBlockChan <- nil
				ch.timer = nil
			}
			ch.bufferLock.Unlock()
		}

	}
}

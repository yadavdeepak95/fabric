/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hbbft

import (
	"encoding/pem"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	etcdtohbbft "github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/consensus"
	honeybadgerbft "github.com/hyperledger/fabric/orderer/consensus/hbbft/honeybadgerbft"
	ab "github.com/hyperledger/fabric/orderer/consensus/hbbft/honeybadgerbft/proto/orderer"

	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
)

const (
	//BatchSize if option is not provided
	BatchSize = 2
)

var submitC chan *submit

//go:generate counterfeiter -o mocks/configurator.go . Configurator

// Configurator is used to configure the communication layer
// when the chain starts.
type Configurator interface {
	Configure(channel string, newNodes []cluster.RemoteNode)
}

//go:generate counterfeiter -o mocks/mock_rpc.go . RPC

// RPC is used to mock the transport layer in tests.
type RPC interface {
	SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error
	SendSubmit(dest uint64, request *orderer.SubmitRequest) error
}

//go:generate counterfeiter -o mocks/mock_blockpuller.go . BlockPuller

// BlockPuller is used to pull blocks from other OSN
type BlockPuller interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	Close()
}

// CreateBlockPuller is a function to create BlockPuller on demand.
// It is passed into chain initializer so that tests could mock this.
type CreateBlockPuller func() (BlockPuller, error)

// Options contains all the configurations relevant to the chain.
type Options struct {
	NodeID    uint64
	Logger    *flogging.FabricLogger
	BatchSize int

	// BlockMetdata and Consenters should only be modified while under lock
	// of hbbftMetadataLock
	BlockMetadata *etcdtohbbft.BlockMetadata
	Consenters    map[uint64]*etcdtohbbft.Consenter

	// MigrationInit is set when the node starts right after consensus-type migration
	MigrationInit bool
	Cert          []byte
}

//TODO change this according to requirements -- most probably not required - thinking of client sending request to all
type submit struct {
	req *orderer.SubmitRequest
}

// Chain implements consensus.Chain interface.
type Chain struct {
	configurator Configurator
	rpc          RPC
	nodeID       uint64
	channelID    string
	ActiveNodes  atomic.Value
	Node         *honeybadgerbft.ChainImpl
	submitC      chan *submit
	haltC        chan struct{} // Signals to goroutines that the chain is halting
	doneC        chan struct{} // Closes when the chain halts
	startC       chan struct{} // Closes when the node is started

	errorCLock sync.RWMutex
	errorC     chan struct{} // returned by Errored()
	// exitChan          chan struct{} // returned by Errored()
	hbbftMetadataLock sync.RWMutex
	configInflight    bool // this is true when there is config block or ConfChange in flight
	support           consensus.ConsenterSupport

	lastBlock    *common.Block
	createPuller CreateBlockPuller // func used to create BlockPuller on demand
	opts         Options
	logger       *flogging.FabricLogger
	haltCallback func()
	// BCCSP instane
	CryptoProvider bccsp.BCCSP
	bc             *blockCreator
}

// NewChain constructs a chain object.
func NewChain(
	support consensus.ConsenterSupport,
	opts Options,
	conf Configurator,
	rpc RPC,
	cryptoProvider bccsp.BCCSP,
	f CreateBlockPuller,
	haltCallback func(),
	observeC chan<- raft.SoftState,
) (*Chain, error) {

	lg := opts.Logger.With("channel", support.ChannelID(), "node", opts.NodeID)

	b := support.Block(support.Height() - 1)
	if b == nil {
		return nil, errors.Errorf("failed to get last block")
	}

	c := &Chain{
		configurator:   conf,
		rpc:            rpc,
		channelID:      support.ChannelID(),
		nodeID:         opts.NodeID,
		submitC:        make(chan *submit),
		haltC:          make(chan struct{}),
		doneC:          make(chan struct{}),
		startC:         make(chan struct{}),
		errorC:         make(chan struct{}),
		support:        support,
		lastBlock:      b,
		createPuller:   f,
		haltCallback:   haltCallback,
		logger:         lg,
		opts:           opts,
		CryptoProvider: cryptoProvider,
	}

	disseminator := &Disseminator{RPC: c.rpc}
	disseminator.UpdateMetadata(nil) // initialize
	c.ActiveNodes.Store([]uint64{})

	c.Node = honeybadgerbft.NewWrapper(c.opts.BatchSize, int(c.nodeID), len(c.opts.Consenters))

	return c, nil
}

// Start instructs the orderer to begin serving the chain and keep it current.
func (c *Chain) Start() {
	c.logger.Infof("Starting Hbbft node")
	fmt.Printf("\n\nCONSENTERS LIST :\n %+v\n\n", c.opts.Consenters)
	if err := c.configureComm(); err != nil {
		c.logger.Errorf("Failed to start chain, aborting: +%v", err)
		close(c.doneC)
		return
	}

	isJoin := c.support.Height() > 1
	if isJoin && c.opts.MigrationInit {
		isJoin = false
		c.logger.Infof("Consensus-type migration detected, starting new raft node on an existing channel; height=%d", c.support.Height())
	}
	c.bc = &blockCreator{
		hash:   protoutil.BlockHeaderHash(c.lastBlock.Header),
		number: c.lastBlock.Header.Number,
		logger: c.logger,
	}

	// close(c.errorC)

	c.Node.Start()
	go c.run()
	go c.send()
	go c.outputtxns()
	close(c.startC)
	// time.Sleep(50 * time.Millisecond)
}

// Order submits normal type transactions for ordering.
func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	// c.Metrics.NormalProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// Configure submits config type transactions for ordering.
func (c *Chain) Configure(env *common.Envelope, configSeq uint64) error {
	// c.Metrics.ConfigProposalsReceived.Add(1)
	fmt.Printf("\n####ISCONFIG[%v]\n", c.isConfig(env))
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

///////////////////////////////////////TODO///////////////////////////////////////////////////////////////

// WaitReady blocks when the chain:
// - is catching up with other nodes using snapshot
//
// In any other case, it returns right away.
func (c *Chain) WaitReady() error {
	if err := c.isRunning(); err != nil {
		return err
	}

	select {
	//TODO check if required and what it does
	case c.submitC <- nil:
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	}

	return nil
}

// Errored returns a channel that closes when the chain stops.
func (c *Chain) Errored() <-chan struct{} {
	c.errorCLock.RLock()
	defer c.errorCLock.RUnlock()
	return c.errorC
}

// Halt stops the chain.
//TODO recheck what I want to do on Halt call.
func (c *Chain) Halt() {
	select {
	case <-c.startC:
	default:
		c.logger.Warnf("Attempted to halt a chain that has not started")
		return
	}

	select {
	// case c.haltC <- struct{}{}:
	case <-c.doneC:
		return
	default:
		close(c.doneC)

		if c.haltCallback != nil {
			c.haltCallback()
		}
	}

}

func (c *Chain) isRunning() error {
	select {
	case <-c.startC:
	default:
		return errors.Errorf("chain is not started")
	}

	select {
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	default:
	}

	return nil
}

////////////////////////////////////////////////////TODO- done/////////////////////////////////////////////

// Consensus passes the given ConsensusRequest message to the raft.Node instance
func (c *Chain) Consensus(req *orderer.ConsensusRequest, sender uint64) error {
	//TODO our teritory here we get hbbft msgs and we can do what ever we want
	if err := c.isRunning(); err != nil {
		return err
	}

	stepMsg := &ab.HoneyBadgerBFTMessage{}
	if err := proto.Unmarshal(req.Payload, stepMsg); err != nil {
		return fmt.Errorf("failed to unmarshal StepRequest payload to Raft Message: %s", err)
	}
	//TODO do metadata no need for now
	// fmt.Printf("\n\nSENDING TO HBBFT NODE[%+v]\n[%+v]\n\n", c.nodeID, stepMsg)
	c.Node.MsgChan.Receive <- stepMsg

	return nil
}

// Submit forwards the incoming request to:
// - the local run goroutine if this is leader
// - the actual leader via the transport mechanism
// The call fails if there's no leader elected yet.
func (c *Chain) Submit(req *orderer.SubmitRequest, sender uint64) error {
	if err := c.isRunning(); err != nil {
		return err
	}
	if sender == c.nodeID {
		for id := range c.opts.Consenters {
			if c.nodeID != id {
				c.rpc.SendSubmit(id, req)
			}
		}
	}
	//TODO : decide whether client will send to all or the nodes will transfer to each other
	select {
	case c.submitC <- &submit{req}:
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	}

	return nil
}

func (c *Chain) send() {
	for {
		select {
		case msg := <-c.Node.MsgChan.Send:
			msg.ChainId = c.channelID
			if msg.Sender == msg.Receiver {
				c.Node.MsgChan.Receive <- &msg
			}
			// fmt.Printf("\n\nCHAIN.GO msg to send in \n%+v\n\n", msg)
			payload, err := proto.Marshal(&msg)
			if err != nil {
				continue
			}
			//todo make a thread for each recver
			c.rpc.SendConsensus(msg.Receiver, &orderer.ConsensusRequest{Channel: c.channelID, Payload: payload})
		case <-c.doneC:
			return
		}
	}
}

func (c *Chain) outputtxns() {
	for {
		select {
		case txn := <-c.Node.MsgChan.Outtxn:
			batches, _, err := c.ordered(&orderer.SubmitRequest{Payload: txn})
			if err != nil {
				c.logger.Errorf("Failed to order message: %s", err)
				continue
			}
			c.propose(c.bc, batches...)

		case <-c.doneC:
			return
		}
	}
}
func (c *Chain) run() {
	//TODO : our forever loop comes here
	// 1. how to send
	// 2. how to recv

	// submitC = c.submitC

	for {
		select {
		case s := <-c.submitC:
			if s == nil {
				// polled by `WaitReady`
				continue
			}
			var err error
			msg := s.req
			seq := c.support.Sequence()
			if c.isConfig(msg.Payload) {
				// ConfigMsg
				if msg.LastValidationSeq < seq {
					c.logger.Warnf("Config message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
					msg.Payload, _, err = c.support.ProcessConfigMsg(msg.Payload)
					if err != nil {
						continue
					}
				}
				c.logger.Info("Received config transaction, pause accepting transaction till it is committed")
				c.Node.Enqueue(msg.Payload, true)
				continue
				// submitC = nil
			}
			// it is a normal message
			if msg.LastValidationSeq < seq {
				c.logger.Warnf("Normal message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
				if _, err := c.support.ProcessNormalMsg(msg.Payload); err != nil {
					continue
				}
			}
			c.Node.Enqueue(msg.Payload, false)

		case <-c.doneC:
			select {
			case <-c.errorC: // avoid closing closed channel
			default:
				close(c.errorC)
			}

			c.logger.Infof("Stop serving requests")

			return
		}
	}
}

func (c *Chain) writeBlock(block *common.Block, index uint64) {
	fmt.Printf("\n\n\n\n\n**********writting BLock***************\n\n\n\n")
	if block.Header.Number > c.lastBlock.Header.Number+1 {
		c.logger.Panicf("Got block [%d], expect block [%d]", block.Header.Number, c.lastBlock.Header.Number+1)
	} else if block.Header.Number < c.lastBlock.Header.Number+1 {
		c.logger.Infof("Got block [%d], expect block [%d], this node was forced to catch up", block.Header.Number, c.lastBlock.Header.Number+1)
		return
	}

	c.lastBlock = block

	c.logger.Infof("Writing block [%d] (Raft index: %d) to ledger", block.Header.Number, index)

	if protoutil.IsConfigBlock(block) {
		c.writeConfigBlock(block, index)
		// submitC = c.submitC
		return
	}
	//TODO : check later if fata to same index bhi de sakte h
	c.hbbftMetadataLock.Lock()
	c.opts.BlockMetadata.RaftIndex = index
	m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
	c.hbbftMetadataLock.Unlock()

	c.support.WriteBlock(block, m)
}

//TODO a thread which handle writing to block and read from hbbft
// Orders the envelope in the `msg` content. SubmitRequest.
// Returns
//   -- batches [][]*common.Envelope; the batches cut,
//   -- pending bool; if there are envelopes pending to be ordered,
//   -- err error; the error encountered, if any.
// It takes care of config messages as well as the revalidation of messages if the config sequence has advanced.
func (c *Chain) ordered(msg *orderer.SubmitRequest) (batches [][]*common.Envelope, pending bool, err error) {
	// seq := c.support.Sequence()

	// if c.isConfig(msg.Payload) {
	fmt.Printf("yes config")
	batch := c.support.BlockCutter().Cut()
	batches = [][]*common.Envelope{}
	if len(batch) != 0 {
		batches = append(batches, batch)
	}
	batches = append(batches, []*common.Envelope{msg.Payload})
	return batches, false, nil
	// }
	// it is a normal message
	// batches, pending = c.support.BlockCutter().Ordered(msg.Payload)
	// return batches, pending, nil

}

//TODO rather then purposing write it
func (c *Chain) propose(bc *blockCreator, batches ...[]*common.Envelope) {
	for _, batch := range batches {
		b := bc.createNextBlock(batch)
		c.logger.Infof("Created block [%d]", b.Header.Number)
		//TODO may need update
		c.writeBlock(b, c.nodeID)
	}

	return
}

func (c *Chain) isConfig(env *common.Envelope) bool {
	h, err := protoutil.ChannelHeader(env)
	if err != nil {
		c.logger.Panicf("failed to extract channel header from envelope")
	}

	return h.Type == int32(common.HeaderType_CONFIG) || h.Type == int32(common.HeaderType_ORDERER_TRANSACTION)
}

func (c *Chain) configureComm() error {
	// Reset unreachable map when communication is reconfigured
	//TODO give a local lock why not?
	// c.Node.unreachableLock.Lock()
	// c.Node.unreachable = make(map[uint64]struct{})
	// c.Node.unreachableLock.Unlock()

	nodes, err := c.remotePeers()
	if err != nil {
		return err
	}

	c.configurator.Configure(c.channelID, nodes)
	return nil
}

func (c *Chain) remotePeers() ([]cluster.RemoteNode, error) {
	c.hbbftMetadataLock.RLock()
	defer c.hbbftMetadataLock.RUnlock()

	var nodes []cluster.RemoteNode
	for nodeID, consenter := range c.opts.Consenters {
		// No need to know yourself
		if nodeID == c.nodeID {
			continue
		}
		serverCertAsDER, err := pemToDER(consenter.ServerTlsCert, nodeID, "server", c.logger)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		clientCertAsDER, err := pemToDER(consenter.ClientTlsCert, nodeID, "client", c.logger)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		nodes = append(nodes, cluster.RemoteNode{
			ID:            nodeID,
			Endpoint:      fmt.Sprintf("%s:%d", consenter.Host, consenter.Port),
			ServerTLSCert: serverCertAsDER,
			ClientTLSCert: clientCertAsDER,
		})
	}
	return nodes, nil
}

func pemToDER(pemBytes []byte, id uint64, certType string, logger *flogging.FabricLogger) ([]byte, error) {
	bl, _ := pem.Decode(pemBytes)
	if bl == nil {
		logger.Errorf("Rejecting PEM block of %s TLS cert for node %d, offending PEM is: %s", certType, id, string(pemBytes))
		return nil, errors.Errorf("invalid PEM block")
	}
	return bl.Bytes, nil
}

//TODO read deep not required abhi
// writeConfigBlock writes configuration blocks into the ledger in
// addition extracts updates about raft replica set and if there
// are changes updates cluster membership as well
func (c *Chain) writeConfigBlock(block *common.Block, index uint64) {
	fmt.Printf("in write config block")
	hdr, err := ConfigChannelHeader(block)
	if err != nil {
		c.logger.Panicf("Failed to get config header type from config block: %s", err)
	}

	switch common.HeaderType(hdr.Type) {

	case common.HeaderType_CONFIG:
		// // 	configMembership := c.detectConfChange(block)

		// 	c.hbbftMetadataLock.Lock()
		// 	c.opts.BlockMetadata.RaftIndex = index
		// 	if configMembership != nil {
		// 		c.opts.BlockMetadata = configMembership.NewBlockMetadata
		// 		c.opts.Consenters = configMembership.NewConsenters
		// 	}
		// 	c.hbbftMetadataLock.Unlock()
		// fmt.Printf("##########################################################################################################################")
		blockMetadataBytes := protoutil.MarshalOrPanic(c.opts.BlockMetadata)

		// 	// write block with metadata
		c.support.WriteConfigBlock(block, blockMetadataBytes)

	// 	if configMembership == nil {
	// 		return
	// 	}

	// 	// update membership
	// 	if configMembership.ConfChange != nil {
	// 		// We need to propose conf change in a go routine, because it may be blocked if raft node
	// 		// becomes leaderless, and we should not block `run` so it can keep consuming applyC,
	// 		// otherwise we have a deadlock.
	// 		go func() {
	// 			// ProposeConfChange returns error only if node being stopped.
	// 			// This proposal is dropped by followers because DisableProposalForwarding is enabled.
	// 			if err := c.Node.ProposeConfChange(context.TODO(), *configMembership.ConfChange); err != nil {
	// 				c.logger.Warnf("Failed to propose configuration update to Raft node: %s", err)
	// 			}
	// 		}()

	// 		c.confChangeInProgress = configMembership.ConfChange

	// 		switch configMembership.ConfChange.Type {
	// 		case raftpb.ConfChangeAddNode:
	// 			c.logger.Infof("Config block just committed adds node %d, pause accepting transactions till config change is applied", configMembership.ConfChange.NodeID)
	// 		case raftpb.ConfChangeRemoveNode:
	// 			c.logger.Infof("Config block just committed removes node %d, pause accepting transactions till config change is applied", configMembership.ConfChange.NodeID)
	// 		default:
	// 			c.logger.Panic("Programming error, encountered unsupported raft config change")
	// 		}

	// 		c.configInflight = true
	// 	} else if configMembership.Rotated() {
	// 		lead := atomic.LoadUint64(&c.lastKnownLeader)
	// 		if configMembership.RotatedNode == lead {
	// 			c.logger.Infof("Certificate of Raft leader is being rotated, attempt leader transfer before reconfiguring communication")
	// 			go func() {
	// 				c.Node.abdicateLeader(lead)
	// 				if err := c.configureComm(); err != nil {
	// 					c.logger.Panicf("Failed to configure communication: %s", err)
	// 				}
	// 			}()
	// 		} else {
	// 			if err := c.configureComm(); err != nil {
	// 				c.logger.Panicf("Failed to configure communication: %s", err)
	// 			}
	// 		}
	// 	}

	case common.HeaderType_ORDERER_TRANSACTION:
		// If this config is channel creation, no extra inspection is needed
		fmt.Printf("pagal")
		c.hbbftMetadataLock.Lock()
		c.opts.BlockMetadata.RaftIndex = index
		m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
		c.hbbftMetadataLock.Unlock()

		c.support.WriteConfigBlock(block, m)

	default:
		c.logger.Panicf("Programming error: unexpected config type: %s", common.HeaderType(hdr.Type))
	}
}

//TODO see later if needed as of now no need to touch it
// func (c *Chain) catchUp(snap *raftpb.Snapshot) error {
// 	b, err := protoutil.UnmarshalBlock(snap.Data)
// 	if err != nil {
// 		return errors.Errorf("failed to unmarshal snapshot data to block: %s", err)
// 	}

// 	if c.lastBlock.Header.Number >= b.Header.Number {
// 		c.logger.Warnf("Snapshot is at block [%d], local block number is %d, no sync needed", b.Header.Number, c.lastBlock.Header.Number)
// 		return nil
// 	}

// 	puller, err := c.createPuller()
// 	if err != nil {
// 		return errors.Errorf("failed to create block puller: %s", err)
// 	}
// 	defer puller.Close()

// 	next := c.lastBlock.Header.Number + 1

// 	c.logger.Infof("Catching up with snapshot taken at block [%d], starting from block [%d]", b.Header.Number, next)

// 	for next <= b.Header.Number {
// 		block := puller.PullBlock(next)
// 		if block == nil {
// 			return errors.Errorf("failed to fetch block [%d] from cluster", next)
// 		}
// 		if protoutil.IsConfigBlock(block) {
// 			c.support.WriteConfigBlock(block, nil)

// 			configMembership := c.detectConfChange(block)

// 			if configMembership != nil && configMembership.Changed() {
// 				c.logger.Infof("Config block [%d] changes consenter set, communication should be reconfigured", block.Header.Number)

// 				c.hbbftMetadataLock.Lock()
// 				c.opts.BlockMetadata = configMembership.NewBlockMetadata
// 				c.opts.Consenters = configMembership.NewConsenters
// 				c.hbbftMetadataLock.Unlock()

// 				if err := c.configureComm(); err != nil {
// 					c.logger.Panicf("Failed to configure communication: %s", err)
// 				}
// 			}
// 		} else {
// 			c.support.WriteBlock(block, nil)
// 		}

// 		c.lastBlock = block
// 		next++
// 	}

// 	c.logger.Infof("Finished syncing with cluster up to and including block [%d]", b.Header.Number)
// 	return nil
// }

//TODO how to use this see as of now not need
// func (c *Chain) detectConfChange(block *common.Block) *MembershipChanges {
// 	// // If config is targeting THIS channel, inspect consenter set and
// 	// // propose raft ConfChange if it adds/removes node.
// 	// configMetadata := c.newConfigMetadata(block)

// 	// if configMetadata == nil {
// 	// 	return nil
// 	// }

// 	// if configMetadata.Options != nil &&
// 	// 	configMetadata.Options.SnapshotIntervalSize != 0 &&
// 	// 	configMetadata.Options.SnapshotIntervalSize != c.sizeLimit {
// 	// 	c.logger.Infof("Update snapshot interval size to %d bytes (was %d)",
// 	// 		configMetadata.Options.SnapshotIntervalSize, c.sizeLimit)
// 	// 	c.sizeLimit = configMetadata.Options.SnapshotIntervalSize
// 	// }

// 	changes, err := ComputeMembershipChanges(c.opts.BlockMetadata, c.opts.Consenters, configMetadata.Consenters)
// 	// if err != nil {
// 	// 	c.logger.Panicf("illegal configuration change detected: %s", err)
// 	// }

// 	// if changes.Rotated() {
// 	// 	c.logger.Infof("Config block [%d] rotates TLS certificate of node %d", block.Header.Number, changes.RotatedNode)
// 	// }

// 	return changes

// }

// func (c *Chain) apply(ents []raftpb.Entry) {
// if len(ents) == 0 {
// 	return
// }

// if ents[0].Index > c.appliedIndex+1 {
// 	c.logger.Panicf("first index of committed entry[%d] should <= appliedIndex[%d]+1", ents[0].Index, c.appliedIndex)
// }

// var position int
// for i := range ents {
// 	switch ents[i].Type {
// 	case raftpb.EntryNormal:
// 		if len(ents[i].Data) == 0 {
// 			break
// 		}

// 		position = i
// 		c.accDataSize += uint32(len(ents[i].Data))

// 		// We need to strictly avoid re-applying normal entries,
// 		// otherwise we are writing the same block twice.
// 		if ents[i].Index <= c.appliedIndex {
// 			c.logger.Debugf("Received block with raft index (%d) <= applied index (%d), skip", ents[i].Index, c.appliedIndex)
// 			break
// 		}
//TODO how to write block
// block := protoutil.UnmarshalBlockOrPanic(ents[i].Data)
// c.writeBlock(block, ents[i].Index)
// c.Metrics.CommittedBlockNumber.Set(float64(block.Header.Number))
//TODO DEEP study required
// 	case raftpb.EntryConfChange:
// 		var cc raftpb.ConfChange
// 		if err := cc.Unmarshal(ents[i].Data); err != nil {
// 			c.logger.Warnf("Failed to unmarshal ConfChange data: %s", err)
// 			continue
// 		}

// 		c.confState = *c.Node.ApplyConfChange(cc)

// 		switch cc.Type {
// 		case raftpb.ConfChangeAddNode:
// 			c.logger.Infof("Applied config change to add node %d, current nodes in channel: %+v", cc.NodeID, c.confState.Nodes)
// 		case raftpb.ConfChangeRemoveNode:
// 			c.logger.Infof("Applied config change to remove node %d, current nodes in channel: %+v", cc.NodeID, c.confState.Nodes)
// 		default:
// 			c.logger.Panic("Programming error, encountered unsupported raft config change")
// 		}

// 		// This ConfChange was introduced by a previously committed config block,
// 		// we can now unblock submitC to accept envelopes.
// 		var configureComm bool
// 		if c.confChangeInProgress != nil &&
// 			c.confChangeInProgress.NodeID == cc.NodeID &&
// 			c.confChangeInProgress.Type == cc.Type {

// 			configureComm = true
// 			c.confChangeInProgress = nil
// 			c.configInflight = false
// 			// report the new cluster size
// 			c.Metrics.ClusterSize.Set(float64(len(c.opts.BlockMetadata.ConsenterIds)))
// 		}

// 		lead := atomic.LoadUint64(&c.lastKnownLeader)
// 		removeLeader := cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == lead
// 		shouldHalt := cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == c.raftID

// 		// unblock `run` go routine so it can still consume Raft messages
// 		go func() {
// 			if removeLeader {
// 				c.logger.Infof("Current leader is being removed from channel, attempt leadership transfer")
// 				c.Node.abdicateLeader(lead)
// 			}

// 			if configureComm && !shouldHalt { // no need to configure comm if this node is going to halt
// 				if err := c.configureComm(); err != nil {
// 					c.logger.Panicf("Failed to configure communication: %s", err)
// 				}
// 			}

// 			if shouldHalt {
// 				c.logger.Infof("This node is being removed from replica set")
// 				c.Halt()
// 				return
// 			}
// 		}()
// 	}

// 	if ents[i].Index > c.appliedIndex {
// 		c.appliedIndex = ents[i].Index
// 	}
// }

// if c.accDataSize >= c.sizeLimit {
// 	b := protoutil.UnmarshalBlockOrPanic(ents[position].Data)

// 	select {
// 	case c.gcC <- &gc{index: c.appliedIndex, state: c.confState, data: ents[position].Data}:
// 		c.logger.Infof("Accumulated %d bytes since last snapshot, exceeding size limit (%d bytes), "+
// 			"taking snapshot at block [%d] (index: %d), last snapshotted block number is %d, current nodes: %+v",
// 			c.accDataSize, c.sizeLimit, b.Header.Number, c.appliedIndex, c.lastSnapBlockNum, c.confState.Nodes)
// 		c.accDataSize = 0
// 		c.lastSnapBlockNum = b.Header.Number
// 		c.Metrics.SnapshotBlockNumber.Set(float64(b.Header.Number))
// 	default:
// 		c.logger.Warnf("Snapshotting is in progress, it is very likely that SnapshotIntervalSize is too small")
// 	}
// }

// return
// }

// func (c *Chain) gc() {
// 	for {
// 		select {
// 		case g := <-c.gcC:
// 			c.Node.takeSnapshot(g.index, g.state, g.data)
// 		case <-c.doneC:
// 			c.logger.Infof("Stop garbage collecting")
// 			return
// 		}
// 	}
// }

// getInFlightConfChange returns ConfChange in-flight if any.
// It returns confChangeInProgress if it is not nil. Otherwise
// it returns ConfChange from the last committed block (might be nil).
// func (c *Chain) getInFlightConfChange() *raftpb.ConfChange {
// 	if c.confChangeInProgress != nil {
// 		return c.confChangeInProgress
// 	}

// 	if c.lastBlock.Header.Number == 0 {
// 		return nil // nothing to failover just started the chain
// 	}

// 	if !protoutil.IsConfigBlock(c.lastBlock) {
// 		return nil
// 	}

// 	// extracting current Raft configuration state
// 	confState := c.Node.ApplyConfChange(raftpb.ConfChange{})

// 	if len(confState.Nodes) == len(c.opts.BlockMetadata.ConsenterIds) {
// 		// Raft configuration change could only add one node or
// 		// remove one node at a time, if raft conf state size is
// 		// equal to membership stored in block metadata field,
// 		// that means everything is in sync and no need to propose
// 		// config update.
// 		return nil
// 	}

// 	return ConfChange(c.opts.BlockMetadata, confState)
// }

// newMetadata extract config metadata from the configuration block
// func (c *Chain) newConfigMetadata(block *common.Block) *etcdraft.ConfigMetadata {
// 	metadata, err := ConsensusMetadataFromConfigBlock(block)
// 	if err != nil {
// 		c.logger.Panicf("error reading consensus metadata: %s", err)
// 	}
// 	return metadata
// }

//TODO needed od check as per consensus requirment

// ValidateConsensusMetadata determines the validity of a
// ConsensusMetadata update during config updates on the channel.
// func (c *Chain) ValidateConsensusMetadata(oldMetadataBytes, newMetadataBytes []byte, newChannel bool) error {
// 	// metadata was not updated
// 	if newMetadataBytes == nil {
// 		return nil
// 	}
// 	if oldMetadataBytes == nil {
// 		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil old metadata")
// 	}

// 	oldMetadata := &etcdraft.ConfigMetadata{}
// 	if err := proto.Unmarshal(oldMetadataBytes, oldMetadata); err != nil {
// 		c.logger.Panicf("Programming Error: Failed to unmarshal old etcdraft consensus metadata: %v", err)
// 	}
// 	newMetadata := &etcdraft.ConfigMetadata{}
// 	if err := proto.Unmarshal(newMetadataBytes, newMetadata); err != nil {
// 		return errors.Wrap(err, "failed to unmarshal new etcdraft metadata configuration")
// 	}

// 	err := CheckConfigMetadata(newMetadata)
// 	if err != nil {
// 		return errors.Wrap(err, "invalid new config metdadata")
// 	}

// 	if newChannel {
// 		// check if the consenters are a subset of the existing consenters (system channel consenters)
// 		set := ConsentersToMap(oldMetadata.Consenters)
// 		for _, c := range newMetadata.Consenters {
// 			if _, exits := set[string(c.ClientTlsCert)]; !exits {
// 				return errors.New("new channel has consenter that is not part of system consenter set")
// 			}
// 		}
// 		return nil
// 	}

// 	// create the dummy parameters for ComputeMembershipChanges
// 	dummyOldBlockMetadata, _ := ReadBlockMetadata(nil, oldMetadata)
// 	dummyOldConsentersMap := CreateConsentersMap(dummyOldBlockMetadata, oldMetadata)
// 	changes, err := ComputeMembershipChanges(dummyOldBlockMetadata, dummyOldConsentersMap, newMetadata.Consenters)
// 	if err != nil {
// 		return err
// 	}

// 	active := c.ActiveNodes.Load().([]uint64)
// 	if changes.UnacceptableQuorumLoss(active) {
// 		return errors.Errorf("%d out of %d nodes are alive, configuration will result in quorum loss", len(active), len(dummyOldConsentersMap))
// 	}

// 	return nil
// }

// func (c *Chain) suspectEviction() bool {
// 	if c.isRunning() != nil {
// 		return false
// 	}

// 	return atomic.LoadUint64(&c.lastKnownLeader) == uint64(0)
// }

// func (c *Chain) newEvictionSuspector() *evictionSuspector {
// 	consenterCertificate := &ConsenterCertificate{
// 		ConsenterCertificate: c.opts.Cert,
// 		CryptoProvider:       c.CryptoProvider,
// 	}

// 	return &evictionSuspector{
// 		amIInChannel:               consenterCertificate.IsConsenterOfChannel,
// 		evictionSuspicionThreshold: c.opts.EvictionSuspicion,
// 		writeBlock:                 c.support.Append,
// 		createPuller:               c.createPuller,
// 		height:                     c.support.Height,
// 		triggerCatchUp:             c.triggerCatchup,
// 		logger:                     c.logger,
// 		halt: func() {
// 			c.Halt()
// 		},
// 	}
// }

// func (c *Chain) triggerCatchup(sn *raftpb.Snapshot) {
// 	select {
// 	case c.snapC <- sn:
// 	case <-c.doneC:
// 	}
// }

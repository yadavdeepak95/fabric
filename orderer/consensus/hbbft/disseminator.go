/*
 Copyright IBM Corp All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package hbbft

import (
	"sync"

	"github.com/hyperledger/fabric-protos-go/orderer"
)

// Disseminator piggybacks cluster metadata, if any, to egress messages.
type Disseminator struct {
	RPC

	l        sync.Mutex
	sent     map[uint64]bool
	metadata []byte
}

//SendConsensus send msg to dest via rpc
func (d *Disseminator) SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error {
	d.l.Lock()
	defer d.l.Unlock()

	if !d.sent[dest] && len(d.metadata) != 0 {
		msg.Metadata = d.metadata
		d.sent[dest] = true
	}

	return d.RPC.SendConsensus(dest, msg)
}

//UpdateMetadata piggybacks the msg going out
func (d *Disseminator) UpdateMetadata(m []byte) {
	d.l.Lock()
	defer d.l.Unlock()

	d.sent = make(map[uint64]bool)
	d.metadata = m
}

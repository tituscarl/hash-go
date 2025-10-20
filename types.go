package hashgo

import "sync"

type HashRing struct {
	sync.RWMutex
	Nodes        []string // sorted node names
	NodeToIP     map[string]string
	NodeToHash   map[string]uint64
	VirtualNodes int               // number of virtual nodes per physical node
	Ring         []uint64          // sorted hash values
	HashToNode   map[uint64]string // hash to node mapping
}

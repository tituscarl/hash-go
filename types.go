package hashgo

import "sync"

type HashRing struct {
	sync.RWMutex
	Nodes        []string // sorted node names
	NodeToIP     map[string]string
	NodeToHash   map[string]uint32 // renamed for clarity
	VirtualNodes int               // number of virtual nodes per physical node
	Ring         []uint32          // sorted hash values
	HashToNode   map[uint32]string // hash to node mapping
}

package hashgo

import "sync"

type HashRing struct {
	sync.RWMutex
	nodes        []string // sorted node names
	nodeToIP     map[string]string
	nodeToHash   map[string]uint32 // renamed for clarity
	virtualNodes int               // number of virtual nodes per physical node
	ring         []uint32          // sorted hash values
	hashToNode   map[uint32]string // hash to node mapping
}

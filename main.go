package hashgo

import (
	"context"
	"fmt"
	"hash/crc32"
	"sort"
)

// NewHashRing creates a new hash ring with virtual nodes for better distribution
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = 150 // default virtual nodes
	}
	return &HashRing{
		nodeToIP:     make(map[string]string),
		nodeToHash:   make(map[string]uint32),
		virtualNodes: virtualNodes,
		hashToNode:   make(map[uint32]string),
		ring:         []uint32{},
	}
}

// AddNode adds a new node to the hash ring with virtual nodes
func (hr *HashRing) addNode(node string, ip string) {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.nodeToIP[node]; ok {
		return // Node already exists
	}

	hr.nodeToIP[node] = ip
	hr.nodes = append(hr.nodes, node)
	sort.Strings(hr.nodes)

	// Add virtual nodes for better distribution
	for i := 0; i < hr.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", node, i)
		hash := crc32.ChecksumIEEE([]byte(virtualKey))
		hr.ring = append(hr.ring, hash)
		hr.hashToNode[hash] = node

		// Store the first hash as the node's primary hash
		if i == 0 {
			hr.nodeToHash[node] = hash
		}
	}

	// Sort the ring after adding virtual nodes
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// RemoveNode removes a node from the hash ring
func (hr *HashRing) removeNode(node string) {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.nodeToIP[node]; !ok {
		return // Node does not exist
	}

	delete(hr.nodeToIP, node)
	delete(hr.nodeToHash, node)

	// Remove virtual nodes from ring
	newRing := make([]uint32, 0, len(hr.ring))
	for _, hash := range hr.ring {
		if hr.hashToNode[hash] != node {
			newRing = append(newRing, hash)
		} else {
			delete(hr.hashToNode, hash)
		}
	}
	hr.ring = newRing

	// Remove the node from the slice
	for i := 0; i < len(hr.nodes); i++ {
		if hr.nodes[i] == node {
			hr.nodes = append(hr.nodes[:i], hr.nodes[i+1:]...)
			break // Only one match possible
		}
	}
}

// GetNode returns the node responsible for the given key
func (hr *HashRing) getNode(key string) (string, error) {
	hr.RLock()
	defer hr.RUnlock()

	if len(hr.ring) == 0 {
		return "", fmt.Errorf("hash ring is empty")
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary search for the first node with hash >= key hash
	index := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= hash
	})

	// Wrap around if needed
	if index == len(hr.ring) {
		index = 0
	}

	return hr.hashToNode[hr.ring[index]], nil
}

// GetNodeIP returns the IP address of the node responsible for the given key
func (hr *HashRing) GetNodeIP(key string) (string, error) {
	node, err := hr.getNode(key)
	if err != nil {
		return "", err
	}

	hr.RLock()
	defer hr.RUnlock()

	ip, ok := hr.nodeToIP[node]
	if !ok {
		return "", fmt.Errorf("IP not found for node: %s", node)
	}

	return ip, nil
}

// GetNodes returns all nodes in the ring
func (hr *HashRing) GetNodes() []string {
	hr.RLock()
	defer hr.RUnlock()

	result := make([]string, len(hr.nodes))
	copy(result, hr.nodes)
	return result
}

// GetNodeForRequest returns the node responsible for the given gRPC request
func (hr *HashRing) GetNodeForRequest(ctx context.Context, key string) (string, error) {
	return hr.getNode(key)
}

// AddNodeForRequest adds a new node for the given gRPC request
func (hr *HashRing) AddNodeForRequest(ctx context.Context, node, ip string) error {
	if node == "" {
		return fmt.Errorf("node name cannot be empty")
	}
	if ip == "" {
		return fmt.Errorf("IP address cannot be empty")
	}
	hr.addNode(node, ip)
	return nil
}

// RemoveNodeForRequest removes a node for the given gRPC request
func (hr *HashRing) RemoveNodeForRequest(ctx context.Context, node string) error {
	hr.removeNode(node)
	return nil
}

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
		NodeToHash:   make(map[string]uint32),
		VirtualNodes: virtualNodes,
		HashToNode:   make(map[uint32]string),
		Ring:         []uint32{},
	}
}

// AddNode adds a new node to the hash ring with virtual nodes
func (hr *HashRing) addNode(ip string) {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.NodeToHash[ip]; ok {
		return // Node already exists
	}

	hr.Nodes = append(hr.Nodes, ip)
	sort.Strings(hr.Nodes)

	// Add virtual nodes for better distribution
	for i := 0; i < hr.VirtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", ip, i)
		hash := crc32.ChecksumIEEE([]byte(virtualKey))
		hr.Ring = append(hr.Ring, hash)
		hr.HashToNode[hash] = ip

		// Store the first hash as the node's primary hash
		if i == 0 {
			hr.NodeToHash[ip] = hash
		}
	}

	// Sort the ring after adding virtual nodes
	sort.Slice(hr.Ring, func(i, j int) bool {
		return hr.Ring[i] < hr.Ring[j]
	})
}

// RemoveNode removes a node from the hash ring
func (hr *HashRing) removeNode(ip string) {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.NodeToHash[ip]; !ok {
		return // Node does not exist
	}

	delete(hr.NodeToHash, ip)

	// Remove virtual nodes from ring
	newRing := make([]uint32, 0, len(hr.Ring))
	for _, hash := range hr.Ring {
		if hr.HashToNode[hash] != ip {
			newRing = append(newRing, hash)
		} else {
			delete(hr.HashToNode, hash)
		}
	}
	hr.Ring = newRing

	// Remove the node from the slice
	for i := 0; i < len(hr.Nodes); i++ {
		if hr.Nodes[i] == ip {
			hr.Nodes = append(hr.Nodes[:i], hr.Nodes[i+1:]...)
			break // Only one match possible
		}
	}
}

// GetNode returns the node responsible for the given key
func (hr *HashRing) getNode(key string) (string, error) {
	hr.RLock()
	defer hr.RUnlock()

	if len(hr.Ring) == 0 {
		return "", fmt.Errorf("hash ring is empty")
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary search for the first node with hash >= key hash
	index := sort.Search(len(hr.Ring), func(i int) bool {
		return hr.Ring[i] >= hash
	})

	// Wrap around if needed
	if index == len(hr.Ring) {
		index = 0
	}

	return hr.HashToNode[hr.Ring[index]], nil
}

// GetNodeIP returns the IP address of the node responsible for the given key
func (hr *HashRing) GetNodeIP(key string) (string, error) {
	// Since node name is now the IP, we can return it directly
	return hr.getNode(key)
}

// GetNodes returns all nodes in the ring
func (hr *HashRing) GetNodes() []string {
	hr.RLock()
	defer hr.RUnlock()

	result := make([]string, len(hr.Nodes))
	copy(result, hr.Nodes)
	return result
}

// GetNodeForRequest returns the node responsible for the given gRPC request
func (hr *HashRing) GetNodeForRequest(ctx context.Context, key string) (string, error) {
	return hr.getNode(key)
}

// AddNodeForRequest adds a new node for the given gRPC request
func (hr *HashRing) AddNodeForRequest(ctx context.Context, ip string) error {
	if ip == "" {
		return fmt.Errorf("IP address cannot be empty")
	}
	hr.addNode(ip)
	return nil
}

// RemoveNodeForRequest removes a node for the given gRPC request
func (hr *HashRing) RemoveNodeForRequest(ctx context.Context, ip string) error {
	hr.removeNode(ip)
	return nil
}

// AssignSubscriptionToNode assigns a subscription to a node based on consistent hashing
func (hr *HashRing) AssignSubscriptionToNode(subscriptionName string) (string, error) {
	ip, err := hr.getNode(subscriptionName)
	if err != nil {
		return "", fmt.Errorf("failed to assign subscription %s: %w", subscriptionName, err)
	}

	return ip, nil
}

// GetSubscriptionNode returns which node handles a given subscription
func (hr *HashRing) GetSubscriptionNode(subscriptionName string) (string, error) {
	return hr.getNode(subscriptionName)
}

// GetSubscriptionNodeWithIP returns the node and IP for a subscription
func (hr *HashRing) GetSubscriptionNodeWithIP(subscriptionName string) (node, ip string, err error) {
	nodeIP, err := hr.getNode(subscriptionName)
	if err != nil {
		return "", "", err
	}

	// Since node name is the IP, return the same value for both
	return nodeIP, nodeIP, nil
}

// AssignMultipleSubscriptions assigns multiple subscriptions and returns a map
func (hr *HashRing) AssignMultipleSubscriptions(subscriptions []string) (map[string]string, error) {
	assignments := make(map[string]string)

	for _, sub := range subscriptions {
		ip, err := hr.getNode(sub)
		if err != nil {
			return nil, fmt.Errorf("failed to assign subscription %s: %w", sub, err)
		}
		assignments[sub] = ip
	}

	return assignments, nil
}

// GetNodeSubscriptions returns all subscriptions assigned to a specific node
func (hr *HashRing) GetNodeSubscriptions(nodeIP string, allSubscriptions []string) []string {
	var nodeSubscriptions []string

	for _, sub := range allSubscriptions {
		ip, err := hr.getNode(sub)
		if err == nil && ip == nodeIP {
			nodeSubscriptions = append(nodeSubscriptions, sub)
		}
	}

	return nodeSubscriptions
}

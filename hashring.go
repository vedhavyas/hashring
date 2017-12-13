package hashring

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"sync"
)

// nodeIdx type implementing Sort Interface
type nodeIdx []uint32

// Len returns the size of nodeIdx
func (idx nodeIdx) Len() int {
	return len(idx)
}

// Swap swaps the ith with jth
func (idx nodeIdx) Swap(i, j int) {
	idx[i], idx[j] = idx[j], idx[i]
}

// Less returns true if ith <= jth else false
func (idx nodeIdx) Less(i, j int) bool {
	return idx[i] <= idx[j]
}

// HashRing to hold the nodes and indexes
type HashRing struct {
	nodes            map[uint32]string // map to idx -> node
	idx              nodeIdx           // sorted indexes
	virtualNodeCount int               // virtual nodes to be inserted
	hash             hash.Hash32
	mu               sync.RWMutex // to protect above fields
}

// New returns a Hash ring with provided virtual node count
func New(virtualNodeCount int) *HashRing {
	return &HashRing{
		nodes:            make(map[uint32]string),
		virtualNodeCount: virtualNodeCount,
		hash:             fnv.New32a(),
	}
}

// Add adds a node to Hash ring
func (hr *HashRing) Add(node string) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualNodeCount; i++ {
		key := fmt.Sprintf("%s:%d", node, i)
		hr.hash.Reset()
		_, err := hr.hash.Write([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to add node: %v", err)
		}

		hkey := hr.hash.Sum32()
		hr.idx = append(hr.idx, hkey)
		hr.nodes[hkey] = node
	}

	sort.Sort(hr.idx)
	return nil
}

// getKeys returns the keys of map m
func getKeys(m map[uint32]string) (idx nodeIdx) {
	for k := range m {
		idx = append(idx, k)
	}

	return idx
}

func (hr *HashRing) Delete(node string) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualNodeCount; i++ {
		key := fmt.Sprintf("%s:%d", node, i)
		hr.hash.Reset()
		_, err := hr.hash.Write([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to delete node: %v", err)
		}

		hkey := hr.hash.Sum32()
		delete(hr.nodes, hkey)
	}

	hr.idx = getKeys(hr.nodes)
	sort.Sort(hr.idx)
	return nil

}

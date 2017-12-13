package hashring

import (
	"reflect"
	"sort"
	"testing"
)

func TestNodeIdx_Sort(t *testing.T) {
	tests := []struct {
		v nodeIdx
		r nodeIdx
	}{
		{
			v: nodeIdx{5, 1, 3, 9, 10, 0, 78},
			r: nodeIdx{0, 1, 3, 5, 9, 10, 78},
		},

		{
			v: nodeIdx{9, 3, 5, 4, 2, 3, 6},
			r: nodeIdx{2, 3, 3, 4, 5, 6, 9},
		},
	}

	for _, c := range tests {
		sort.Sort(c.v)
		if !reflect.DeepEqual(c.v, c.r) {
			t.Fatalf("expected %v but got %v\n", c.r, c.v)
		}
	}
}

func TestHashRing_AddNode(t *testing.T) {
	tests := []struct {
		virtualNodeCount int
		nodes            []string
		requiredCount    int
	}{
		{
			virtualNodeCount: 1,
			nodes:            []string{"abc_node"},
			requiredCount:    1,
		},

		{
			virtualNodeCount: 3,
			nodes:            []string{"abc_node"},
			requiredCount:    3,
		},

		{
			virtualNodeCount: 4,
			nodes:            []string{"abc_1", "abc_2"},
			requiredCount:    8,
		},

		{
			virtualNodeCount: 1,
			nodes:            []string{"abc_1", "abc_2", "abc_3"},
			requiredCount:    3,
		},
	}

	testContains := func(hr *HashRing, nodes []string) bool {
		result := true
		for _, node := range nodes {
			count := 0
			for _, v := range hr.nodes {
				if v == node {
					count++
				}
			}

			result = result && (count == hr.virtualNodeCount)
		}

		return result
	}

	for _, c := range tests {
		hr := New(c.virtualNodeCount)
		for _, node := range c.nodes {
			hr.Add(node)
		}

		if !(testContains(hr, c.nodes) && len(hr.idx) == c.virtualNodeCount*len(c.nodes)) {
			t.Fatalf("node count didn't match: %+v\n", hr)
		}
	}
}

func TestHashRing_Delete(t *testing.T) {
	vcount := 3
	nodes := []string{"abc_1", "abc_node", "abc_2", "abc_3"}

	hr := New(vcount)
	for _, n := range nodes {
		hr.Add(n)
	}

	testContains := func(hr *HashRing, nodes []string) bool {
		result := true
		for _, node := range nodes {
			count := 0
			for _, v := range hr.nodes {
				if v == node {
					count++
				}
			}

			result = result && (count == hr.virtualNodeCount)
		}

		return result
	}

	tests := []struct {
		nodes         []string
		requiredCount int
	}{
		{
			nodes:         []string{"abc_node"},
			requiredCount: 9,
		},

		{
			nodes:         []string{"abc_3"},
			requiredCount: 6,
		},

		{
			nodes:         []string{"abc_1", "abc_2"},
			requiredCount: 0,
		},
	}

	for _, c := range tests {
		for _, n := range c.nodes {
			hr.Delete(n)
		}

		if testContains(hr, c.nodes) || len(hr.idx) != c.requiredCount {
			t.Fatalf("delete failed: %+v\n", hr)
		}
	}
}

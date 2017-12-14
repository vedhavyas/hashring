# Consistent Hashing
Consistent hashing is a special kind of hashing such that when a hash table is resized, only K/n keys need to be remapped on average, where K is the number of keys, and n is the number of slots.
This academic paper from 1997 introduced the term "consistent hashing" as a way of distributing requests among a changing population of Web servers. Each slot is then represented by a node in a distributed system. The addition (joins) and removal (leaves/failures) of nodes only requires K/n items to be re-shuffled when the number of slots/nodes change


## Usage

`import "github.com/vedhavyas/hashring"`

#### type HashRing

```go
type HashRing struct {
}
```

HashRing to hold the nodes and indexes

#### func  New

```go
func New(replicaCount int, hash hash.Hash32) *HashRing
```
New returns a Hash ring with provided virtual node count and hash If hash is
nil, fvn32a is used instead

#### func (*HashRing) Add

```go
func (hr *HashRing) Add(node string) error
```
Add adds a node to Hash ring

#### func (*HashRing) Delete

```go
func (hr *HashRing) Delete(node string) error
```
Delete deletes the nodes from hash ring

#### func (*HashRing) Locate

```go
func (hr *HashRing) Get(key string) (node string, err error)
```
Locate returns the node for a given key

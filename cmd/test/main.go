/*
Package to test the distribution of keys across nodes
*/

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/spaolacci/murmur3"
	"github.com/vedhavyas/hashring"
)

var (
	replicaCount = flag.Int("rc", 4, "replication count")
	nodeCount    = flag.Int("nc", 8, "node count")
	keyCount     = flag.Int("kc", 6000000, "key count")
)

func main() {
	flag.Parse()
	hr := hashring.New(*replicaCount, murmur3.New32())
	nodeMap := make(map[string]int)
	for i := 0; i < *nodeCount; i++ {
		err := hr.Add(fmt.Sprintf("node-%d", i))
		if err != nil {
			log.Fatal(err)
		}
	}

	fd, _ := os.Open("/usr/share/dict/words")
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for i := 0; i < *keyCount; i++ {
		ok := scanner.Scan()
		if !ok {
			break
		}

		n, err := hr.Locate(scanner.Text())
		if err != nil {
			log.Fatal(err)
		}
		nodeMap[n.(string)]++
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}

	for k, v := range nodeMap {
		fmt.Printf("%s: %d\n", k, v)
	}
}

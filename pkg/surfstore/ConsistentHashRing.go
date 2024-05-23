package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	fmt.Println("BlockId: ", blockId)
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			fmt.Println("ServerMap: ", c.ServerMap[hashes[i]])
			return c.ServerMap[hashes[i]]
		}
	}
	fmt.Println("ServerMap: ", c.ServerMap[hashes[0]])
	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentRing := &ConsistentHashRing{ServerMap: make(map[string]string)}
	serverMap := make(map[string]string)
	for _, serverPort := range serverAddrs {
		serverHash := consistentRing.Hash("blockstore" + serverPort)
		consistentRing.ServerMap[serverHash] = serverPort
	}
	fmt.Println("ServerMap: ", serverMap)
	return &ConsistentHashRing{ServerMap: consistentRing.ServerMap}
}

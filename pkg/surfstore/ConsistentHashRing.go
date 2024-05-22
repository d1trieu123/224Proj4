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
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			return c.ServerMap[hashes[i]]
		}
	}
	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func Hash_to_string(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	for _, serverPort := range serverAddrs {
		serverHash := Hash_to_string("blockstore" + serverPort)
		serverMap[serverHash] = serverPort
	}
	fmt.Println("ServerMap: ", serverMap)
	return &ConsistentHashRing{ServerMap: serverMap}
}

package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	return c.ServerMap[c.Hash(blockId)]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	hashes := []string{}
	for _, serverName := range serverAddrs {
		serverHash := Hash_to_string(serverName)
		serverMap[serverHash] = serverName
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)
	return &ConsistentHashRing{ServerMap: serverMap}
}

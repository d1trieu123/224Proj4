package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) { //MY CODE
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) { //MY CODE
	fileName := fileMetaData.Filename
	fileInfo, ok := m.FileMetaMap[fileName]
	if !ok {
		// fmt.Println("METASTORE: UPDATEFILE: File not found, creating new file")
		newFile := &FileMetaData{Filename: fileMetaData.GetFilename(), Version: fileMetaData.GetVersion(), BlockHashList: fileMetaData.GetBlockHashList()}
		m.FileMetaMap[fileName] = newFile
		return &Version{Version: 1}, nil
	}
	if fileInfo.Version != fileMetaData.Version-1 {
		return &Version{Version: -1}, fmt.Errorf("Version mismatch")

	}
	m.FileMetaMap[fileName] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil
}

// Given a list of block hashes,
// find out which block server they belong to.
// Returns a mapping from block server address to block hashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	BlockMap := map[string]*BlockHashes{}

	for _, blockHash := range blockHashesIn.Hashes {
		blockStoreAddr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := BlockMap[blockStoreAddr]; !ok {
			BlockMap[blockStoreAddr] = &BlockHashes{Hashes: []string{}}
		}
		BlockMap[blockStoreAddr].Hashes = append(BlockMap[blockStoreAddr].Hashes, blockHash)
	}

	return &BlockStoreMap{BlockStoreMap: BlockMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}

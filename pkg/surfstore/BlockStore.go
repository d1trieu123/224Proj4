package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) { //MY CODE
	// fmt.Println("BLOCKSTORE.GETBLOCK: Getting block with hash: ", blockHash.Hash)
	val, ok := bs.BlockMap[blockHash.Hash]
	if blockHash.Hash == "-1" {
		emptyBlock := &Block{BlockData: []byte{}, BlockSize: -1}
		// fmt.Println("BLOCKSTORE.GETBLOCK: EMPTY FILE")
		return emptyBlock, nil
	}
	if blockHash.Hash == "0" {
		emptyBlock := &Block{BlockData: nil, BlockSize: 0}
		// fmt.Println("BLOCKSTORE.GETBLOCK: DELETED FILE")
		return emptyBlock, nil

	}

	if !ok {
		// fmt.Println("BLOCKSTORE.GETBLOCK: Block not found")
		return nil, fmt.Errorf("Block not found")
	}
	// fmt.Println("BLOCKSTORE.GETBLOCK: Block found")
	return val, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) { //MY CODE
	// fmt.Println("BLOCKSTORE.PUTBLOCK: Adding block")
	// fmt.Println("BLOCKSTORE.PUTBLOCK: Block data: ", block.BlockData)
	var hashString string
	blockData := block.BlockData
	if blockData == nil {
		// fmt.Println("BLOCKSTORE.PUTBLOCK: EMPTY FILE")
		hashString = "-1"
	} else {
		hashString = GetBlockHashString(blockData)
	}

	// fmt.Println("BLOCKSTORE.PUTBLOCK: Hash string: ", hashString)
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) { //MY CODE
	// fmt.Println("BLOCKSTORE.MISSINGBLOCKS: Checking for missing blocks")
	missingBlocks := &BlockHashes{}
	for key, _ := range bs.BlockMap {
		found := false
		for _, hash := range blockHashesIn.Hashes {
			if key == hash {
				found = true
				break
			}
		}
		if !found {
			missingBlocks.Hashes = append(missingBlocks.Hashes, key)
		}
	}
	// fmt.Println("BLOCKSTORE.MISSINGBLOCKS: Missing blocks found")
	return missingBlocks, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	for key, _ := range bs.BlockMap {
		fmt.Println("BLOCKSTORE.GETBLOCKHASHES: ", key)
	}
	blockHashes := &BlockHashes{}
	for key, _ := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, key)
	}
	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

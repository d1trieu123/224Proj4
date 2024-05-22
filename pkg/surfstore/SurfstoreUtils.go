package surfstore

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	baseDir := client.BaseDir
	blockSize := client.BlockSize
	hashToData := make(map[string][]byte)

	// open the base directory
	directory, err := os.Open(baseDir)
	if os.IsNotExist(err) {
		log.Fatal("Base directory does not exist")
	}
	defer directory.Close()
	//process all files in the base directory
	localDirectory := make(map[string][]string)
	files, err := directory.Readdir(-1)
	if err != nil {
		log.Fatal("Error reading base directory")
	}
	for _, file := range files {
		// open the file
		fileName := file.Name()
		ok := validateFileName(fileName)
		if !ok {
			// fmt.Println(fileName)
			// fmt.Println("Invalid file name")
			continue
		}
		localDirectory[fileName] = []string{}
		filePath := baseDir + "/" + file.Name()
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Error opening file")
		}
		defer file.Close()
		// read the file
		var hashList []string
		// if the file is empty, add a -1
		stat, err := file.Stat()
		if err != nil {
			log.Fatal("Error getting file stats")
		}
		if stat.Size() == 0 {
			// fmt.Println("EMPTY FILE: " + fileName)
			hashList = append(hashList, "-1")
			localDirectory[fileName] = hashList
			continue
		}
		for {
			fileData := make([]byte, blockSize)
			n, err := file.Read(fileData)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal("Error reading file:", err)
			}
			// Use only the read data
			fileData = fileData[:n]

			// Hash the block
			hash := GetBlockHashString(fileData)
			hashList = append(hashList, hash)
			localDirectory[fileName] = hashList
			// fmt.Println("FILE DATA: ", fileData)
			// fmt.Println("HASH: ", hash)
			hashToData[hash] = fileData
		}

	}
	// load the meta file as a local map (localIndex)
	localIndex, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatal("Error loading meta file")
	}
	// fmt.Println("LOCAL INDEX LOADED FROM INDEX FILE")
	PrintMetaMap(localIndex)

	// compare the local index with the local directory
	updatedLocalIndex := make(map[string]*FileMetaData)
	for fileName, hashList := range localDirectory {
		// if the file is not in the index file, add it
		if _, ok := localIndex[fileName]; !ok {
			updatedLocalIndex[fileName] = &FileMetaData{Filename: fileName, Version: 1, BlockHashList: hashList}
		} else { // file is in the index file, check the hash list
			var newHashList []string
			localIndexHashList := localIndex[fileName].BlockHashList
			changed := false
			for i := 0; i < len(hashList); i++ { //loop through directory hash list
				spot := hashList[i]
				if i < len(localIndexHashList) { // directory hash list is longer than index hash list
					localIndexSpot := localIndexHashList[i]
					if spot != localIndexSpot { // hash at the same spot is different, version change and update hash
						changed = true
						newHashList = append(newHashList, spot)
					} else { // hash at the same spot is the same, keep the hash
						newHashList = append(newHashList, localIndexSpot)
					}
				} else { // once the index hash list is exhausted, add the rest of the directory hash list, version change
					changed = true
					newHashList = append(newHashList, spot)
				}
			}
			if changed { // if the hash list is different, update the index file with new hash and version
				updatedLocalIndex[fileName] = &FileMetaData{Filename: fileName, Version: localIndex[fileName].Version + 1, BlockHashList: newHashList}
			} else { // if the hash list is the same, keep the index file, no version change
				updatedLocalIndex[fileName] = localIndex[fileName]
			}

		}
	}
	// check if files have been deleted in the local directory (file in index but not in directory use tombstone)
	localIndexkeys := getIndexKeys(localIndex)
	localDirectorykeys := getDirectoryKeys(localDirectory)
	for _, key := range localIndexkeys {
		if !contains(localDirectorykeys, key) {
			if localIndex[key].BlockHashList[0] == "0" {
				// fmt.Println("FILE HAS ALREADY BEEN DELETED")
				updatedLocalIndex[key] = localIndex[key]
			} else {
				// fmt.Println("FILE DELETED")
				tombstoneList := []string{}
				tombstoneList = append(tombstoneList, "0")
				updatedLocalIndex[key] = &FileMetaData{Filename: key, Version: localIndex[key].Version + 1, BlockHashList: tombstoneList}
			}
		}
	}
	// print the updated local index
	// fmt.Println("UPDATED LOCAL INDEX AFTER COMPARISON WITH LOCAL DIRECTORY")
	PrintMetaMap(updatedLocalIndex)

	//load the remote index from the server
	hostPort := client.MetaStoreAddr
	rpcClient := NewSurfstoreRPCClient(hostPort, baseDir, blockSize)
	remoteIndex := make(map[string]*FileMetaData)
	err = rpcClient.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatal("Error getting remote index")
	}
	fmt.Println("REMOTE INDEX LOADED FROM SERVER")
	PrintMetaMap(remoteIndex)
	remoteBlockStoreAddrs := []string{}
	//load all the block store address
	err = rpcClient.GetBlockStoreAddrs(&remoteBlockStoreAddrs)
	if err != nil {
		log.Fatal("Error getting block store address")
	}

	for _, blockStoreAddr := range remoteBlockStoreAddrs {
		fmt.Println("Block Store Address: ", blockStoreAddr)
	}

	//compare the remote index with the updated local index
	finalMetaMap := make(map[string]*FileMetaData)
	for fileName, localFileMetaData := range updatedLocalIndex {
		blockMap := make(map[string][]string)
		err = rpcClient.GetBlockStoreMap(localFileMetaData.BlockHashList, &blockMap)
		if err != nil {
			log.Fatal("Error getting block store map")
		}
		// file in local index but not remote index, add it to the remote index
		if _, ok := remoteIndex[fileName]; !ok {
			// fmt.Println("FILE: " + fileName + " IN LOCAL INDEX BUT NOT IN REMOTE INDEX")
			err = rpcClient.UpdateFile(localFileMetaData, &localFileMetaData.Version)
			if err != nil {
				log.Fatal("Error updating file")
			}
			addToBlockStore(rpcClient, localFileMetaData, remoteBlockStoreAddrs, hashToData, blockMap)
			finalMetaMap[fileName] = localFileMetaData

		} else { // file in both local and remote index, compare the version and hash list and update as necessary
			remoteFileMetaData := remoteIndex[fileName]
			// fmt.Println("FILE " + fileName + " IN BOTH LOCAL AND REMOTE INDEX")
			if localFileMetaData.Version < remoteFileMetaData.Version {
				filePath := baseDir + "/" + fileName
				// fmt.Println("LOCAL FILE " + fileName + " IS OUT OF DATE")
				if remoteFileMetaData.BlockHashList[0] == "0" {
					// fmt.Println("FILE HAS BEEN DELETED" + fileName)
					finalMetaMap[fileName] = remoteFileMetaData
					err := os.Remove(filePath)
					if err != nil {
						log.Fatal("Error deleting file")
					}
					continue
				}
				// edit the file in the base directory to match the remote file
				editFile(filePath, remoteFileMetaData, rpcClient)
				finalMetaMap[fileName] = remoteFileMetaData
			} else if localFileMetaData.Version == remoteFileMetaData.Version { //check hash list for differences this means someone else has pushed first
				// fmt.Println("LOCAL FILE " + fileName + " HAS SAME VERSION AS REMOTE FILE, CHECKING HASH LIST")
				localHashList := localFileMetaData.BlockHashList
				remoteHashList := remoteFileMetaData.BlockHashList
				if sameList(localHashList, remoteHashList) {
					// fmt.Println("HASH LISTS ARE THE SAME NO CHANGE NEEDED")
					finalMetaMap[fileName] = localFileMetaData
				} else {
					// edit the file in the base directory to match the remote file
					filePath := baseDir + "/" + fileName
					editFile(filePath, remoteFileMetaData, rpcClient)
					finalMetaMap[fileName] = remoteFileMetaData
				}

			} else if localFileMetaData.Version == remoteFileMetaData.Version+1 { //update the file in the remote index (garbage collection doesnt occur )
				// fmt.Println("LOCAL FILE " + fileName + " IS VERSION AHEAD OF REMOTE FILE, UPDATING REMOTE FILE")
				err = rpcClient.UpdateFile(localFileMetaData, &localFileMetaData.Version)
				if err != nil {
					log.Fatal("Error updating file")
				}
				addToBlockStore(rpcClient, localFileMetaData, remoteBlockStoreAddrs, hashToData, blockMap)
				finalMetaMap[fileName] = localFileMetaData
			} else { //invalid version number
				// fmt.Println("INVALID VERSION NUMBER")
			}
		}

	}
	// file in remote index but not local index, reconstitute the file in base directory then add to local index
	localUpdatedIndexkeys := getIndexKeys(updatedLocalIndex)
	remoteIndexkeys := getIndexKeys(remoteIndex)
	for _, key := range remoteIndexkeys {
		if !contains(localUpdatedIndexkeys, key) {
			// fmt.Println("REMOTE INDEX FILE: " + key + " NOT IN LOCAL INDEX")
			remoteFileMetaData := remoteIndex[key]
			if remoteFileMetaData.BlockHashList[0] == "0" {
				// fmt.Println("FILE HAS BEEN DELETED" + key)
				finalMetaMap[key] = remoteFileMetaData
				continue
			}
			// reconstitute the file in the base directory
			filePath := baseDir + "/" + key
			file, err := os.Create(filePath)
			if err != nil {
				log.Fatal("Error creating file")
			}
			defer file.Close()
			writeToFile(remoteFileMetaData.BlockHashList, file, rpcClient)
			// for _, hash := range remoteFileMetaData.BlockHashList {
			// 	var block Block
			// 	err := rpcClient.GetBlock(hash, remoteBlockStoreAddr, &block)
			// 	if err != nil {
			// 		log.Fatal("Error getting block" + err.Error())
			// 	}
			// 	// add the block to the block list and write to the file
			// 	blockData := block.BlockData
			// 	_, err = file.Write(blockData)
			// 	if err != nil {
			// 		log.Fatal("Error writing block to file")
			// 	}
			// }
			// add the file to the local index
			finalMetaMap[key] = remoteFileMetaData
		}
	}

	//write all changes to index.db
	err = WriteMetaFile(finalMetaMap, baseDir)
	if err != nil {
		log.Fatal("Error writing meta file")
	}
	// fmt.Println("INDEX.DB updated")

}

func validateFileName(fileName string) bool {
	if fileName == DEFAULT_META_FILENAME || strings.Contains(fileName, ",") || strings.Contains(fileName, "/") {
		return false
	}
	return true
}

func getDirectoryKeys(m map[string][]string) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}
func getIndexKeys(m map[string]*FileMetaData) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func writeToFile(hashList []string, file *os.File, client RPCClient) {
	blockMap := make(map[string][]string)
	err := client.GetBlockStoreMap(hashList, &blockMap)
	if err != nil {
		log.Fatal("Error getting block store map")
	}
	for _, hash := range hashList {
		for blockStoreAddr, hashList := range blockMap {
			if contains(hashList, hash) {
				var block Block
				err := client.GetBlock(hash, blockStoreAddr, &block)
				if err != nil {
					log.Fatal("Error getting block" + err.Error())
				}
				// add the block to the block list and write to the file
				blockData := block.BlockData
				_, err = file.Write(blockData)
				if err != nil {
					log.Fatal("Error writing block to file")
				}
			}
		}
	}

}

func addToBlockStore(client RPCClient, fileMetaData *FileMetaData, blockStoreAddrs []string, hashToData map[string][]byte, blockMap map[string][]string) {
	fmt.Println("ADDING FILE TO BLOCK STORE")
	var success bool
	for blockStoreAddr, hashList := range blockMap {
		for _, hash := range hashList {
			var block Block
			blockData := hashToData[hash]
			block.BlockData = blockData
			block.BlockSize = int32(len(blockData))
			err := client.PutBlock(&block, blockStoreAddr, &success)
			if err != nil {
				log.Fatal("Error putting block" + err.Error())
			}
		}
	}
}

func editFile(filePath string, fileMetaData *FileMetaData, client RPCClient) {
	//delete the current file

	err := os.Remove(filePath)
	if err != nil {
		fmt.Println("file doesn't need to be removed")
	}
	//reconstitute the file
	if fileMetaData.BlockHashList[0] == "0" {
		// fmt.Println("FILE HAS BEEN DELETED")
		return
	}
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Error creating file")
	}
	defer file.Close()
	writeToFile(fileMetaData.BlockHashList, file, client)
	// for _, hash := range fileMetaData.BlockHashList {
	// 	var block Block
	// 	blockStoreAddr := RPCClient.GetResponsibleServer(hash)
	// 	err := client.GetBlock(hash, blockStoreAddr, &block)
	// 	if err != nil {
	// 		log.Fatal("Error getting block" + err.Error())
	// 	}
	// 	// add the block to the block list
	// 	blockData := block.BlockData
	// 	_, err = file.Write(blockData)
	// 	if err != nil {
	// 		log.Fatal("Error writing block to file")
	// 	}
	// }
}

func sameList(list1 []string, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}
	for i := range list1 {
		if list1[i] != list2[i] {
			return false
		}
	}
	return true
}

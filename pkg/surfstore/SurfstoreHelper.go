package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `CREATE TABLE IF NOT EXISTS indexes(
	fileName TEXT, 
	version INT,
	hashIndex INT,
	hashValue TEXT
);`

const insertTuple string = `insert into indexes(fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

const getFileMetaDataTable string = `select fileName, version, hashIndex, hashValue 
							  from indexes order by fileName`

const updateTuple string = `update indexes set hashValue = ?, version = ? where fileName = ? and version = ? and hashIndex = ?;`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	for _, fileMeta := range fileMetas {
		for i, hash := range fileMeta.BlockHashList {
			_, err = statement.Exec(fileMeta.Filename, fileMeta.Version, i, hash)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName from indexes;`

const getTuplesByFileName string = `select version, hashIndex, hashValue from indexes where fileName = ? order by version, hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fmt.Println(metaFilePath)

	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		fmt.Println("META FILE DOES NOT EXIST")
		return fileMetaMap, nil
	}
	fmt.Println("META FILE EXISTS")

	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		fmt.Println(err.Error())
	}

	// create table in .db file
	rows, err := db.Query(getFileMetaDataTable)
	if err != nil {
		log.Fatal("Error When Querying Meta")
	}
	var fileName string
	var version int
	var hashIndex int
	var hashValue string

	for rows.Next() {
		fmt.Println("READING ROW")
		rows.Scan(&fileName, &version, &hashIndex, &hashValue)
		fmt.Println(fileName, version, hashIndex, hashValue)
		if _, ok := fileMetaMap[fileName]; !ok {
			fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: int32(version), BlockHashList: []string{}}
		}
		fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, hashValue)
	}

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

package index

import (
	"github.com/roseduan/rosedb/storage"
)

// Indexer the data index info, stored in skip list.
//
// Indexer 保存数据的索引信息，如 wal 文件的 ID/Offset
type Indexer struct {
	Meta   *storage.Meta // metadata info.
	FileId uint32        // the file id of storing the data.
	Offset int64         // entry data query start position.
}

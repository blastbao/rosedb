package rosedb

import (
	"encoding/binary"
	"github.com/roseduan/rosedb/index"
	"github.com/roseduan/rosedb/storage"
	"io"
	"os"
	"sync"
)

// 原子性
//
// 原子性指的是的事务执行的完整性，要么全部成功，要么全部失败，不能停留在中间状态。
// 要实现原子性其实不难，可以借助 rosedb 的写入特性来解决。
//
// 先来回顾一下 rosedb 数据写入的基本流程，两个步骤：
// 	首先数据会先落磁盘，保证可靠性；
// 	然后更新内存中的索引信息。
//
// 对于一个事务操作，要保证原子性，可以先将需要写入的数据在内存中暂存，然后在提交事务的时候，一次性写入到磁盘文件当中。
// 这样存在一个问题，那就是在批量写入磁盘的时候出错，或者系统崩溃了怎么办？
// 也就是说可能有一些数据已经写入成功，有一些写入失败了。
// 按照原子性的定义，这一次事务没有提交完成，是无效的，那么应该怎么知道已经写入的数据是无效的呢？
//
// 目前 rosedb 采用了一种最容易理解，也是比较简单的一种办法来解决这个问题。
//
// 具体做法是这样的：
//	每一次事务开始时，都会分配一个全局唯一的事务 id，需要写入的数据都会带上这个事务 id 并写入到文件。
//	当所有的数据写入磁盘完成之后，将这个事务 id 单独存起来（也是写入到一个文件当中）。
// 	在数据库启动的时候，会先加载这个文件中的所有事务 id，维护到一个集合当中，称之为已提交的事务 id。
//
//	这样的话，就算数据在批量写入时出错，由于没有存放对应的事务 id，
// 	所以在数据库启动并取出数据构建索引的时候（回忆一下 rosedb 的启动流程），
//	能够检查到数据对应的事务 id 没有在已提交事务 id 集合当中，所以会认为这些数据无效。
//
//	大多数 LSM 流派的 k-v 都是利用类似的思路来保证事务的原子性，
//	例如 rocksdb 是将事务中所有的写入都存放到了一个 WriteBatch 中，在事务提交的时候一次性写入。



// 隔离性
//
// 目前 rosedb 支持两种事务类型：读写事务和只读事务。
// 只能同时开启一个读写事务，只读事务则可以同时开启多个。
//
// 在这种模式下，读会加读锁，写会加写锁，也就是说，读写会互斥，不能同时进行。
// 可以理解为这是四种隔离级别中的串行化，它的优点是简单易实现，缺点是并发能力差。
//
// 需要说明的是，目前的这种实现在后面大概率会进行调整，
// 我的设想是可以使用快照隔离的方式来支持读提交或者可重复读，
// 这样数据读取能够读到历史版本，不会造成写操作的阻塞，只不过在实现上要复杂得多了。
//

const (
	txIdLen = 8
)

type (

	// Txn is a rosedb Transaction.
	// You can begin a read-write transaction by calling Txn, and read-only transaction by calling TxnView.
	// Transaction will be committed or rollback automatically in method Txn and TxnView.
	Txn struct {

		// transaction id is an increasing number to mark a unique tx.
		id uint64
		db *RoseDB
		wg *sync.WaitGroup

		// strEntries is the written entries for String, they are different from other data structures.
		strEntries map[string]*storage.Entry

		// writeEntries is the written entries for List, Hash, Set, ZSet.
		writeEntries []*storage.Entry

		// skipIds save entries`s index that don`t need be processed.
		skipIds map[int]struct{}

		// all keys stored in a map to handle duplicate entries.
		keysMap map[string]int

		// indicate how many data structures will be handled in transaction.
		// 事务状态，表示有多少数据结构被影响。
		dsState uint16

		isFinished bool
	}

	// TxnMeta represents some transaction info while tx is running.
	TxnMeta struct {
		// MaxTxId the max tx id now.
		// 最大事务 ID ，每次创建事务会 +1 。
		MaxTxId uint64

		// ActiveTxIds committed tx ids in active files.
		// 活跃事务 ID 列表
		ActiveTxIds *sync.Map

		// CommittedTxIds save the transaction ids that has been successfully committed.
		// 已提交事务 ID 列表
		CommittedTxIds map[uint64]struct{}

		// a file for saving committed tx ids.
		// 事务文件，保存所有已提交的 txID
		txnFile *TxnFile
	}

	// TxnFile a single file in disk to save committed transaction ids.
	TxnFile struct {
		File   *os.File // file.
		Offset int64    // write offset.
	}
)

// Txn execute a transaction including read and write.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rollback.
func (db *RoseDB) Txn(fn func(tx *Txn) error) (err error) {

	if db.isClosed() {
		return ErrDBIsClosed
	}

	txn := db.NewTransaction()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		txn.Rollback()
		return
	}
	return
}

// TxnView execute a transaction including read only.
func (db *RoseDB) TxnView(fn func(tx *Txn) error) (err error) {
	if db.isClosed() {
		return ErrDBIsClosed
	}
	txn := db.NewTransaction()

	dTypes := []DataType{String, List, Hash, Set, ZSet}
	unlockFunc := txn.db.lockMgr.RLock(dTypes...)
	defer unlockFunc()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}
	txn.finished()
	return
}

// NewTransaction create a new transaction, don`t support concurrent execution of transactions now.
// So you can only open a read-write transaction at the same time.
// For read-only transactions, you can execute multiple, and any write operations will be omitted.
func (db *RoseDB) NewTransaction() *Txn {
	// 最大事务 ID +1
	db.mu.Lock()
	defer func() {
		db.txnMeta.MaxTxId += 1
		db.mu.Unlock()
	}()

	// 创建事务对象
	return &Txn{
		id:         db.txnMeta.MaxTxId + 1,				// 事务 ID
		db:         db,									// 关联 DB
		wg:         new(sync.WaitGroup),				//
		strEntries: make(map[string]*storage.Entry),	//
		keysMap:    make(map[string]int),				//
		skipIds:    make(map[int]struct{}),				//
	}
}

// Commit commit the transaction.
func (tx *Txn) Commit() (err error) {

	// 状态检查
	if tx.db.isClosed() {
		return ErrDBIsClosed
	}

	// 结束事务
	defer tx.finished()

	// 无变更
	if len(tx.strEntries) == 0 && len(tx.writeEntries) == 0 {
		return
	}

	// 获取事务影响的数据结构
	dTypes := tx.getDTypes()

	// 循环加锁
	unlockFunc := tx.db.lockMgr.Lock(dTypes...)
	defer unlockFunc()

	// write entry into db files.
	var indexes []*index.Indexer
	if len(tx.strEntries) > 0 && len(tx.writeEntries) > 0 {
		tx.wg.Add(2)
		go func() {
			defer tx.wg.Done()
			// 将 tx.strEntries 写入到 wal 文件
			if indexes, err = tx.writeStrEntries(); err != nil {
				return
			}
		}()
		go func() {
			defer tx.wg.Done()
			// 将 tx.writeEntries 写入到 wal 文件
			if err = tx.writeOtherEntries(); err != nil {
				return
			}
		}()
		tx.wg.Wait()
		if err != nil {
			return err
		}
	} else {
		// 将 tx.strEntries 写入到 wal 文件
		if indexes, err = tx.writeStrEntries(); err != nil {
			return
		}
		// 将 tx.writeEntries 写入到 wal 文件
		if err = tx.writeOtherEntries(); err != nil {
			return
		}
	}

	// sync the db file for transaction durability.
	//
	// 将当前活跃文件刷盘
	if tx.db.config.Sync {
		if err := tx.db.Sync(); err != nil {
			return err
		}
	}

	// mark the transaction is committed.
	//
	// 把 txId 保存到事务文件中。
	if err = tx.db.MarkCommit(tx.id); err != nil {
		return
	}

	// build indexes.
	//
	// 构建索引(内存操作)
	for _, idx := range indexes {
		if err = tx.db.buildIndex(tx.strEntries[string(idx.Meta.Key)], idx, false); err != nil {
			return
		}
	}

	// 构建索引(内存操作)
	for _, entry := range tx.writeEntries {
		if err = tx.db.buildIndex(entry, nil, false); err != nil {
			return
		}
	}

	return
}

// Rollback finished current transaction.
func (tx *Txn) Rollback() {
	tx.finished()
}

// MarkCommit write the tx id into txn file.
//
// 把 txId 保存到事务文件中。
func (db *RoseDB) MarkCommit(txId uint64) (err error) {

	// 将 txId(8B) 序列化成 []byte
	buf := make([]byte, txIdLen)
	binary.BigEndian.PutUint64(buf[:], txId)

	// 将 txId 写入到 txnFile 的 offset 处
	offset := db.txnMeta.txnFile.Offset
	_, err = db.txnMeta.txnFile.File.WriteAt(buf, offset)
	if err != nil {
		return
	}

	// 更新 offset
	db.txnMeta.txnFile.Offset += int64(len(buf))

	// 刷盘
	if db.config.Sync {
		if err = db.txnMeta.txnFile.File.Sync(); err != nil {
			return
		}
	}

	return
}

// LoadTxnMeta load txn meta info, committed tx id.
func LoadTxnMeta(path string) (txnMeta *TxnMeta, err error) {
	txnMeta = &TxnMeta{
		CommittedTxIds: make(map[uint64]struct{}),
		ActiveTxIds:    new(sync.Map),
	}

	var (
		file    *os.File
		maxTxId uint64
		stat    os.FileInfo
	)
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}
	if stat, err = file.Stat(); err != nil {
		return
	}
	txnMeta.txnFile = &TxnFile{
		File:   file,
		Offset: stat.Size(),
	}

	if txnMeta.txnFile.Offset > 0 {
		var offset int64
		for {
			buf := make([]byte, txIdLen)
			_, err = file.ReadAt(buf, offset)
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}
			txId := binary.BigEndian.Uint64(buf)
			if txId > maxTxId {
				maxTxId = txId
			}
			txnMeta.CommittedTxIds[txId] = struct{}{}
			offset += txIdLen
		}
	}
	txnMeta.MaxTxId = maxTxId
	return
}

func (tx *Txn) finished() {
	tx.strEntries = nil
	tx.writeEntries = nil

	tx.skipIds = nil
	tx.keysMap = nil

	tx.isFinished = true
	return
}

func (tx *Txn) writeStrEntries() (indexes []*index.Indexer, err error) {
	if len(tx.strEntries) == 0 {
		return
	}

	// 遍历 string entries ，逐个写入到 wal 文件中，返回待索引信息。
	for _, entry := range tx.strEntries {

		// 将 string entry 存储到 wal 文件
		if err = tx.db.store(entry); err != nil {
			return
		}

		// 获取该 wal 文件信息
		activeFile, err := tx.db.getActiveFile(String)
		if err != nil {
			return nil, err
		}

		// generate index.
		//
		// 生成索引项
		indexes = append(indexes, &index.Indexer{
			Meta: &storage.Meta{
				Key: entry.Meta.Key,
			},
			FileId: activeFile.Id,
			Offset: activeFile.Offset - int64(entry.Size()),
		})
	}

	return
}

func (tx *Txn) writeOtherEntries() (err error) {
	if len(tx.writeEntries) == 0 {
		return
	}

	for i, entry := range tx.writeEntries {
		if _, ok := tx.skipIds[i]; ok {
			continue
		}
		if err = tx.db.store(entry); err != nil {
			return
		}
	}
	return
}

func (tx *Txn) putEntry(e *storage.Entry) (err error) {
	if e == nil {
		return
	}
	if tx.db.isClosed() {
		return ErrDBIsClosed
	}
	if tx.isFinished {
		return ErrTxIsFinished
	}

	switch e.GetType() {
	case String:
		tx.strEntries[string(e.Meta.Key)] = e
	default:
		tx.writeEntries = append(tx.writeEntries, e)
	}
	tx.setDsState(e.GetType())
	return
}

func (tx *Txn) setDsState(dType DataType) {
	tx.dsState = tx.dsState | (1 << dType)
}

func (tx *Txn) getDTypes() (dTypes []uint16) {
	// string
	if (tx.dsState&(1<<String))>>String == 1 {
		dTypes = append(dTypes, String)
	}
	// list
	if (tx.dsState&(1<<List))>>List == 1 {
		dTypes = append(dTypes, List)
	}
	// hash
	if (tx.dsState&(1<<Hash))>>Hash == 1 {
		dTypes = append(dTypes, Hash)
	}
	// set
	if (tx.dsState&(1<<Set))>>Set == 1 {
		dTypes = append(dTypes, Set)
	}
	// zset
	if (tx.dsState&(1<<ZSet))>>ZSet == 1 {
		dTypes = append(dTypes, ZSet)
	}
	return
}

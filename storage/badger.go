package storage

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
)

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStorage(path string) (*BadgerStorage, error) {
	opt := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &BadgerStorage{
		db: db,
	}, err
}

func (s *BadgerStorage) Close() error {
	return s.db.Close()
}

func (s *BadgerStorage) Begin(update bool) *BadgerTxn {
	return &BadgerTxn{
		txn:    s.db.NewTransaction(update),
		prefix: nil,
	}
}

type BadgerTxn struct {
	txn    *badger.Txn
	prefix []byte
}

func (txn *BadgerTxn) Commit() error {
	return txn.txn.Commit()
}

func (txn *BadgerTxn) Rollback() {
	txn.txn.Discard()
}

func (txn *BadgerTxn) WithPrefix(prefix []byte) {
	txn.prefix = prefix
}

// Get returns the corresponding value of the key. If the key not found, ErrKeyNotFound should be returned.
func (txn *BadgerTxn) Get(key []byte) ([]byte, error) {
	item, err := txn.txn.Get(concat(txn.prefix, key))
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, ErrKeyNotFound
	}

	return item.ValueCopy(nil)
}

// Set sets the corresponding value of the key.
func (txn *BadgerTxn) Set(key []byte, val []byte) error {
	return txn.txn.Set(concat(txn.prefix, key), val)
}

// Delete deletes the corresponding value of the key.
func (txn *BadgerTxn) Delete(key []byte) error {
	return txn.txn.Delete(concat(txn.prefix, key))
}

func concat(prefix []byte, key []byte) []byte {
	return bytes.Join([][]byte{prefix, key}, nil)
}

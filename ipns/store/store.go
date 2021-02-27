package store

import (
	"bytes"
	"encoding/gob"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/textileio/go-threads/core/thread"
)

var (
	dsPrefix = ds.NewKey("/ipns")
	dsCid    = dsPrefix.ChildString("cid")
)

type Key struct {
	Name      string
	Cid       string
	ThreadID  thread.ID
	CreatedAt time.Time
}

type Store struct {
	store ds.TxnDatastore
}

func NewStore(store ds.TxnDatastore) *Store {
	return &Store{store: store}
}

func (s *Store) Create(name, cid string, threadID thread.ID) error {
	txn, err := s.store.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Discard()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(Key{
		Name:      name,
		Cid:       cid,
		ThreadID:  threadID,
		CreatedAt: time.Now(),
	}); err != nil {
		return err
	}

	// Add key value
	if err := txn.Put(dsPrefix.ChildString(name), buf.Bytes()); err != nil {
		return err
	}

	// Add "indexes"
	if err := txn.Put(dsCid.ChildString(cid), []byte(name)); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *Store) Get(name string) (*Key, error) {
	val, err := s.store.Get(dsPrefix.ChildString(name))
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(val)
	dec := gob.NewDecoder(&buf)
	var key Key
	if err := dec.Decode(&key); err != nil {
		return nil, err
	}
	return &key, nil
}

func (s *Store) GetByCid(cid string) (*Key, error) {
	txn, err := s.store.NewTransaction(true)
	if err != nil {
		return nil, err
	}
	defer txn.Discard()

	val, err := txn.Get(dsCid.ChildString(cid))
	if err != nil {
		return nil, err
	}
	return s.Get(string(val))
}

func (s *Store) Delete(name string) error {
	txn, err := s.store.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Discard()

	key, err := s.Get(name)
	if err != nil {
		return err
	}

	// Delete "indexes"
	if err := txn.Delete(dsCid.ChildString(key.Cid)); err != nil {
		return err
	}

	// Delete key value
	if err := txn.Delete(dsPrefix.ChildString(key.Name)); err != nil {
		return err
	}

	return txn.Commit()
}

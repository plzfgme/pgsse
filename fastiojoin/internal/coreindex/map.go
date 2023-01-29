package coreindex

import (
	"bytes"
	"encoding/binary"

	"github.com/plzfgme/pgsse/storage"
)

func sigmaGet(r storage.Retriever, w []byte) ([]byte, uint64, error) {
	b, err := r.Get(sigmaKey(w))
	if err != nil {
		return nil, 0, err
	}

	st := b[:stSize]
	c := binary.BigEndian.Uint64(b[stSize:])

	return st, c, nil
}

func sigmaSet(m storage.RetrieverMutator, w []byte, st []byte, c uint64) error {
	buf := &bytes.Buffer{}
	_, err := buf.Write(st)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.BigEndian, c)
	if err != nil {
		return err
	}

	err = m.Set(sigmaKey(w), buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func sigmaKey(w []byte) []byte {
	return bytes.Join([][]byte{[]byte("s"), w}, nil)
}

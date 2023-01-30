package coreindex

import (
	"bytes"
	"encoding/binary"

	"github.com/plzfgme/pgsse/storage"
)

var sigmaUniqueKey = []byte("unique")

func sigmaGet(r storage.Retriever) ([]byte, uint64, error) {
	b, err := r.Get(sigmaKey(sigmaUniqueKey))
	if err != nil {
		return nil, 0, err
	}

	st := b[:stSize]
	c := binary.BigEndian.Uint64(b[stSize:])

	return st, c, nil
}

func sigmaSet(m storage.RetrieverMutator, st []byte, c uint64) error {
	buf := &bytes.Buffer{}
	_, err := buf.Write(st)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.BigEndian, c)
	if err != nil {
		return err
	}

	err = m.Set(sigmaKey(sigmaUniqueKey), buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func sigmaKey(w []byte) []byte {
	return bytes.Join([][]byte{[]byte("s"), w}, nil)
}

package coreindex

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/plzfgme/pgsse/storage"
)

type ClientOptions struct {
	key []byte
}

func NewClientOptions(key []byte) *ClientOptions {
	return &ClientOptions{
		key: key,
	}
}

type Client struct {
	f  *fPRF
	h1 *h1Hash
	h2 *h2Hash
	h  *hHash
}

func NewClient(opt *ClientOptions) (*Client, error) {
	if len(opt.key) != KeySize {
		return nil, ErrKeySize
	}

	f, _ := newFPRF(opt.key[:16], opt.key[16:])
	h1 := newH1Hash()
	h2 := newH2Hash()
	h := newHHash()

	return &Client{
		f:  f,
		h1: h1,
		h2: h2,
		h:  h,
	}, nil
}

func (client *Client) GenUpdateToken(rm storage.RetrieverMutator, w []byte, token []byte) ([]byte, error) {
	st, c, err := sigmaGet(rm)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			st = make([]byte, 16)
			_, err := rand.Read(st)
			if err != nil {
				return nil, err
			}
			c = 0
		} else {
			return nil, err
		}
	}

	input := binary.BigEndian.AppendUint64(st, c+1)
	u, err := client.h1.Eval(input)
	if err != nil {
		return nil, err
	}

	hw, err := client.h.Eval(w)
	if err != nil {
		return nil, err
	}
	tw := client.f.Eval(hw)

	ePart1, err := buildEPart1(tw, token)
	if err != nil {
		return nil, err
	}
	ePart2, err := client.h2.Eval(input)
	if err != nil {
		return nil, err
	}
	e := xor160Bytes(ePart1, ePart2)

	err = sigmaSet(rm, st, c+1)
	if err != nil {
		return nil, err
	}

	tkn := buildUpdateToken(u, e)

	return tkn, nil
}

func (client *Client) GenSearchToken(rm storage.RetrieverMutator) ([]byte, error) {
	st, c, err := sigmaGet(rm)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	var kw []byte
	if c != 0 {
		kw = st
		st = make([]byte, 16)
		_, err := rand.Read(st)
		if err != nil {
			return nil, err
		}

		err = sigmaSet(rm, st, 0)
		if err != nil {
			return nil, err
		}
	} else {
		kw = nil
	}

	tkn, err := buildSearchToken(kw, c)
	if err != nil {
		return nil, err
	}

	return tkn, nil
}

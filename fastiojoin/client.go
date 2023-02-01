package fastiojoin

import (
	"bytes"
	"encoding/binary"

	"github.com/plzfgme/pgsse/internal/fastio64"
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
	f *fPRF
	h *hHash

	c *fastio64.Client
}

func NewClient(opt *ClientOptions) (*Client, error) {
	if len(opt.key) != KeySize {
		return nil, ErrKeySize
	}

	f, _ := newFPRF(opt.key[:16], opt.key[16:32])
	h := newHHash()

	c, err := fastio64.NewClient(fastio64.NewClientOptions(opt.key[32:]))
	if err != nil {
		return nil, err
	}

	return &Client{f: f, h: h, c: c}, nil
}

func (c *Client) GenInsertAToken(rm storage.RetrieverMutator, w []byte, id int64) ([]byte, error) {
	return c.genInsertToken(rm, sideA, w, id)
}

func (c *Client) GenInsertBToken(rm storage.RetrieverMutator, w []byte, id int64) ([]byte, error) {
	return c.genInsertToken(rm, sideB, w, id)
}

func (c *Client) genInsertToken(rm storage.RetrieverMutator, side byte, w []byte, id int64) ([]byte, error) {
	tw, err := c.buildTW(w)
	if err != nil {
		return nil, err
	}

	fastioID, err := buildFASTIOID(tw, side, id)
	if err != nil {
		return nil, err
	}

	return c.c.GenInsertToken(rm, uniqueFASTIOW, fastioID)
}

func (c *Client) GenDeleteAToken(rm storage.RetrieverMutator, w []byte, id int64) ([]byte, error) {
	return c.genDeleteToken(rm, sideA, w, id)
}

func (c *Client) GenDeleteBToken(rm storage.RetrieverMutator, w []byte, id int64) ([]byte, error) {
	return c.genDeleteToken(rm, sideB, w, id)
}

func (c *Client) genDeleteToken(rm storage.RetrieverMutator, side byte, w []byte, id int64) ([]byte, error) {
	tw, err := c.buildTW(w)
	if err != nil {
		return nil, err
	}

	fastioID, err := buildFASTIOID(tw, side, id)
	if err != nil {
		return nil, err
	}

	return c.c.GenDeleteToken(rm, uniqueFASTIOW, fastioID)
}

func (c *Client) GenSearchToken(rm storage.RetrieverMutator) ([]byte, error) {
	return c.c.GenSearchToken(rm, uniqueFASTIOW)
}

func (c *Client) buildTW(w []byte) ([]byte, error) {
	hw, err := c.h.Eval(w)
	if err != nil {
		return nil, err
	}

	return c.f.Eval(hw), nil
}

func buildFASTIOID(tw []byte, side byte, id int64) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := buf.Write(tw)
	if err != nil {
		return nil, err
	}
	err = buf.WriteByte(side)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(make([]byte, fastio64.IDSize-buf.Len()))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err
}

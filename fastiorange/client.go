package fastiorange

import (
	"github.com/plzfgme/pgsse/fastio"
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
	c *fastio.Client
}

func NewClient(opt *ClientOptions) (*Client, error) {
	c, err := fastio.NewClient(fastio.NewClientOptions(opt.key))
	if err != nil {
		return nil, err
	}

	return &Client{
		c: c,
	}, nil
}

func (c *Client) GenInsertToken(rm storage.RetrieverMutator, v uint64, id int64) ([][]byte, error) {
	ws, err := InsertKeywords(v)
	if err != nil {
		return nil, err
	}
	tkns := make([][]byte, len(ws))
	for i, w := range ws {
		tkn, err := c.c.GenInsertToken(rm, w, id)
		if err != nil {
			return nil, err
		}
		tkns[i] = tkn
	}

	return tkns, nil
}

func (c *Client) GenDeleteToken(rm storage.RetrieverMutator, v uint64, id int64) ([][]byte, error) {
	ws, err := InsertKeywords(v)
	if err != nil {
		return nil, err
	}
	tkns := make([][]byte, len(ws))
	for i, w := range ws {
		tkn, err := c.c.GenDeleteToken(rm, w, id)
		if err != nil {
			return nil, err
		}
		tkns[i] = tkn
	}

	return tkns, nil
}

func (c *Client) GenSearchToken(rm storage.RetrieverMutator, a, b uint64) ([][]byte, error) {
	ws, err := BRCSearchKeywords(a, b)
	if err != nil {
		return nil, err
	}
	tkns := make([][]byte, len(ws))
	for i, w := range ws {
		tkn, err := c.c.GenSearchToken(rm, w)
		if err != nil {
			return nil, err
		}
		tkns[i] = tkn
	}

	return tkns, nil
}

package fastiojoin

import (
	"github.com/plzfgme/pgsse/fastiojoin/internal/coreindex"
	"github.com/plzfgme/pgsse/fastiojoin/internal/sideindex"
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
	core *coreindex.Client
	side *sideindex.Client
}

func NewClient(opt *ClientOptions) (*Client, error) {
	if len(opt.key) != KeySize {
		return nil, ErrKeySize
	}

	core, err := coreindex.NewClient(coreindex.NewClientOptions(opt.key[:coreindex.KeySize]))
	if err != nil {
		return nil, err
	}
	side, err := sideindex.NewClient(sideindex.NewClientOptions(opt.key[coreindex.KeySize : coreindex.KeySize+sideindex.KeySize]))
	if err != nil {
		return nil, err
	}

	return &Client{
		core: core,
		side: side,
	}, nil
}

func (c *Client) GenInsertAToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, error) {
	return c.genInsertToken(rm, sideindex.SideA, w, id)
}

func (c *Client) GenInsertBToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, error) {
	return c.genInsertToken(rm, sideindex.SideB, w, id)
}

func (c *Client) genInsertToken(rm storage.RetrieverMutator, side sideindex.Side, w []byte, id uint64) ([]byte, []byte, error) {
	sideTkn, err := c.side.GenInsertToken(rm, side, w, id)
	if err != nil {
		return nil, nil, err
	}

	searchTkn, err := c.side.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, err
	}

	coreTkn, err := c.core.GenUpdateToken(rm, w, searchTkn)
	if err != nil {
		return nil, nil, err
	}

	return coreTkn, sideTkn, nil
}

func (c *Client) GenDeleteAToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, error) {
	return c.genDeleteToken(rm, sideindex.SideA, w, id)
}

func (c *Client) GenDeleteBToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, error) {
	return c.genInsertToken(rm, sideindex.SideB, w, id)
}

func (c *Client) genDeleteToken(rm storage.RetrieverMutator, side sideindex.Side, w []byte, id uint64) ([]byte, []byte, error) {
	sideTkn, err := c.side.GenDeleteToken(rm, side, w, id)
	if err != nil {
		return nil, nil, err
	}

	searchTkn, err := c.side.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, err
	}

	coreTkn, err := c.core.GenUpdateToken(rm, w, searchTkn)
	if err != nil {
		return nil, nil, err
	}

	return coreTkn, sideTkn, nil
}

func (c *Client) GenSearchToken(rm storage.RetrieverMutator) ([]byte, error) {
	return c.core.GenSearchToken(rm)
}

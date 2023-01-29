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
	abc *coreindex.Client
	ac  *sideindex.Client
	bc  *sideindex.Client
}

func NewClient(opt *ClientOptions) (*Client, error) {
	if len(opt.key) != KeySize {
		return nil, ErrKeySize
	}

	abc, err := coreindex.NewClient(coreindex.NewClientOptions(opt.key[:coreindex.KeySize]))
	if err != nil {
		return nil, err
	}
	ac, err := sideindex.NewClient(sideindex.NewClientOptions(opt.key[coreindex.KeySize : coreindex.KeySize+sideindex.KeySize]))
	if err != nil {
		return nil, err
	}
	bc, err := sideindex.NewClient(sideindex.NewClientOptions(opt.key[coreindex.KeySize+sideindex.KeySize : coreindex.KeySize+2*sideindex.KeySize]))
	if err != nil {
		return nil, err
	}

	return &Client{
		abc: abc,
		ac:  ac,
		bc:  bc,
	}, nil
}

func (c *Client) GenInsertAToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, []byte, error) {
	return c.genInsertToken(rm, w, id, true)
}

func (c *Client) GenInsertBToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, []byte, error) {
	return c.genInsertToken(rm, w, id, false)
}

func (c *Client) genInsertToken(rm storage.RetrieverMutator, w []byte, id uint64, aOrB bool) ([]byte, []byte, []byte, error) {
	aITkn := []byte(nil)
	bITkn := []byte(nil)

	if aOrB {
		tkn, err := c.ac.GenInsertToken(rm, w, id)
		if err != nil {
			return nil, nil, nil, err
		}
		aITkn = tkn
	} else {
		tkn, err := c.bc.GenInsertToken(rm, w, id)
		if err != nil {
			return nil, nil, nil, err
		}
		bITkn = tkn
	}

	aSTkn, err := c.ac.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, nil, err
	}
	bSTkn, err := c.bc.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, nil, err
	}

	abTkn, err := c.abc.GenInsertToken(rm, w, aSTkn, bSTkn)
	if err != nil {
		return nil, nil, nil, err
	}

	return abTkn, aITkn, bITkn, nil
}

func (c *Client) GenDeleteAToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, []byte, error) {
	return c.genDeleteToken(rm, w, id, true)
}

func (c *Client) GenDeleteBToken(rm storage.RetrieverMutator, w []byte, id uint64) ([]byte, []byte, []byte, error) {
	return c.genDeleteToken(rm, w, id, false)
}

func (c *Client) genDeleteToken(rm storage.RetrieverMutator, w []byte, id uint64, aOrB bool) ([]byte, []byte, []byte, error) {
	aITkn := []byte(nil)
	bITkn := []byte(nil)

	if aOrB {
		tkn, err := c.ac.GenDeleteToken(rm, w, id)
		if err != nil {
			return nil, nil, nil, err
		}
		aITkn = tkn
	} else {
		tkn, err := c.bc.GenDeleteToken(rm, w, id)
		if err != nil {
			return nil, nil, nil, err
		}
		bITkn = tkn
	}

	aSTkn, err := c.ac.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, nil, err
	}
	bSTkn, err := c.bc.GenSearchToken(rm, w)
	if err != nil {
		return nil, nil, nil, err
	}

	abTkn, err := c.abc.GenDeleteToken(rm, w, aSTkn, bSTkn)
	if err != nil {
		return nil, nil, nil, err
	}

	return abTkn, aITkn, bITkn, nil
}

func (c *Client) GenJoinSearchToken(rm storage.RetrieverMutator, w []byte) ([]byte, error) {
	return c.abc.GenSearchToken(rm, w)
}

package fastio

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
)

type fPRF struct {
	iv     []byte
	block  cipher.Block
	stream cipher.Stream
}

func newFPRF(key []byte, iv []byte) (*fPRF, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)

	return &fPRF{
		iv:     iv,
		block:  block,
		stream: stream,
	}, nil
}

func (f *fPRF) Eval(input []byte) []byte {
	dst := make([]byte, len(input))
	f.stream.XORKeyStream(dst, input)
	f.stream = cipher.NewCTR(f.block, f.iv)

	return dst
}

type h1Hash struct {
	h hash.Hash
}

func newH1Hash() *h1Hash {
	return &h1Hash{
		hmac.New(sha256.New, []byte("1")),
	}
}

func (h1 *h1Hash) Eval(input []byte) ([]byte, error) {
	_, err := h1.h.Write(input)
	if err != nil {
		return nil, err
	}
	result := h1.h.Sum(nil)
	h1.h.Reset()

	return result, nil
}

type h2Hash struct {
	h hash.Hash
}

func newH2Hash() *h2Hash {
	return &h2Hash{
		hmac.New(sha256.New, []byte("2")),
	}
}

func (h2 *h2Hash) Eval(input []byte) ([]byte, error) {
	_, err := h2.h.Write(input)
	if err != nil {
		return nil, err
	}
	result := h2.h.Sum(nil)
	h2.h.Reset()

	return result, nil
}

type hHash struct {
	h hash.Hash
}

func newHHash() *hHash {
	return &hHash{
		h: sha256.New(),
	}
}

func (h *hHash) Eval(input []byte) ([]byte, error) {
	_, err := h.h.Write(input)
	if err != nil {
		return nil, err
	}
	result := h.h.Sum(nil)
	h.h.Reset()

	return result, nil
}

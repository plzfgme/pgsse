package fastiojoin

import (
	"crypto/aes"
	"crypto/cipher"
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

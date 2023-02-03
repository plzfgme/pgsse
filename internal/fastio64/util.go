package fastio64

import (
	"bytes"
	"encoding/binary"

	"github.com/lukechampine/fastxor"
)

func concatBytes(s [][]byte) []byte {
	return bytes.Join(s, nil)
}

func buildEPart1(flag byte, id []byte) []byte {
	return concatBytes([][]byte{{flag}, id})
}

func buildUpdateToken(u, e []byte) []byte {
	return concatBytes([][]byte{u, e})
}

func buildSearchToken(tw, kw []byte, c uint64) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := buf.Write(tw)
	if err != nil {
		return nil, err
	}
	if kw != nil {
		err := buf.WriteByte(byte(1))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(kw)
		if err != nil {
			return nil, err
		}
	} else {
		err := buf.WriteByte(byte(0))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(make([]byte, kwSize))
		if err != nil {
			return nil, err
		}
	}
	err = binary.Write(buf, binary.BigEndian, c)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func xor64Bytes(part1, part2 []byte) []byte {
	e := make([]byte, eSize)
	fastxor.Block(e[0:16], part1[0:16], part2[0:16])
	fastxor.Block(e[16:32], part1[16:32], part2[16:32])
	fastxor.Block(e[32:48], part1[32:48], part2[32:48])
	fastxor.Block(e[48:64], part1[48:64], part2[48:64])

	return e
}

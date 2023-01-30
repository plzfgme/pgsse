package coreindex

import (
	"bytes"
	"encoding/binary"

	"github.com/lukechampine/fastxor"
)

func concatBytes(s [][]byte) []byte {
	return bytes.Join(s, nil)
}

func buildEPart1(tw []byte, token []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := buf.Write(tw)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(token)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(make([]byte, eSize-buf.Len()))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func buildUpdateToken(u, e []byte) []byte {
	return concatBytes([][]byte{u, e})
}

func buildSearchToken(kw []byte, c uint64) ([]byte, error) {
	buf := &bytes.Buffer{}
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
	err := binary.Write(buf, binary.BigEndian, c)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func xor160Bytes(part1, part2 []byte) []byte {
	e := make([]byte, eSize)
	fastxor.Block(e[0:16], part1[0:16], part2[0:16])
	fastxor.Block(e[16:32], part1[16:32], part2[16:32])
	fastxor.Block(e[32:48], part1[32:48], part2[32:48])
	fastxor.Block(e[48:64], part1[48:64], part2[48:64])
	fastxor.Block(e[64:80], part1[64:80], part2[64:80])
	fastxor.Block(e[80:96], part1[80:96], part2[80:96])
	fastxor.Block(e[96:112], part1[96:112], part2[96:112])
	fastxor.Block(e[112:128], part1[112:128], part2[112:128])
	fastxor.Block(e[128:144], part1[128:144], part2[128:144])
	fastxor.Block(e[144:160], part1[144:160], part2[144:160])

	return e
}

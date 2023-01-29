package fastiorange

import (
	"bytes"
	"encoding/binary"
)

func InsertKeywords(v uint64) ([][]byte, error) {
	res := make([][]byte, 64)
	var err error
	for i := 0; i < 64; i++ {
		res[i], err = makeKeyword(v>>i, i)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func BRCSearchKeywords(a, b uint64) ([][]byte, error) {
	if a == b {
		kw, err := makeKeyword(a, 0)
		if err != nil {
			return nil, err
		}
		return [][]byte{kw}, nil
	}

	result := make([][]byte, 0)
	var t int
	for t = 64 - 1; t >= 0; t-- {
		if bit(a, t) != bit(b, t) {
			break
		}
	}
	if isLastNBitsAllZero(a, t+1) {
		if isLastNBitsAllOne(b, t+1) {
			kw, err := makeKeyword(a>>(t+1), t+1)
			if err != nil {
				return nil, err
			}
			result = append(result, kw)
		} else {
			kw, err := makeKeyword(a>>t, t)
			if err != nil {
				return nil, err
			}
			result = append(result, kw)
		}
	} else {
		var u int
		for u = 0; u < t; u++ {
			if bit(a, u) == 1 {
				break
			}
		}
		for i := t - 1; i >= u+1; i-- {
			if bit(a, i) == 0 {
				kw, err := makeKeyword((a>>(i+1))<<1+1, i)
				if err != nil {
					return nil, err
				}
				result = append(result, kw)
			}
		}
		kw, err := makeKeyword(a>>u, u)
		if err != nil {
			return nil, err
		}
		result = append(result, kw)
	}

	if isLastNBitsAllOne(b, t+1) {
		kw, err := makeKeyword(b>>t, t)
		if err != nil {
			return nil, err
		}
		result = append(result, kw)
	} else {
		var v int
		for v = 0; v < t; v++ {
			if bit(b, v) == 0 {
				break
			}
		}
		for i := t - 1; i >= v+1; i-- {
			if bit(b, i) == 1 {
				kw, err := makeKeyword((b>>(i+1))<<1, i)
				if err != nil {
					return nil, err
				}
				result = append(result, kw)
			}
		}
		kw, err := makeKeyword(b>>v, v)
		if err != nil {
			return nil, err
		}
		result = append(result, kw)
	}

	return result, nil
}

func bit(x uint64, n int) uint {
	if x&(1<<n) != 0 {
		return 1
	} else {
		return 0
	}
}

func isLastNBitsAllZero(x uint64, n int) bool {
	return x == ((x >> n) << n)
}

func isLastNBitsAllOne(x uint64, n int) bool {
	return (x - ((x >> n) << n)) == ((1 << n) - 1)
}

func makeKeyword(prefix uint64, suffixLen int) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, prefix)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, uint8(suffixLen))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

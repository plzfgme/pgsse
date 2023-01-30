package sideindex

const KeySize = 32

const (
	eSize  = 32
	stSize = 16
	kwSize = stSize
)

const (
	flagAdd    byte = 0
	flagDelete byte = 1
)

type Side byte

const (
	SideA Side = 0
	SideB Side = 1
)

package fastiojoin

import "github.com/plzfgme/pgsse/internal/fastio64"

const KeySize = 32 + fastio64.KeySize

const (
	sideA byte = 0
	sideB byte = 1
)

var uniqueFASTIOW = []byte("unique")

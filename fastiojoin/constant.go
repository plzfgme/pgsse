package fastiojoin

import (
	"github.com/plzfgme/pgsse/fastiojoin/internal/coreindex"
	"github.com/plzfgme/pgsse/fastiojoin/internal/sideindex"
)

const KeySize = coreindex.KeySize + sideindex.KeySize

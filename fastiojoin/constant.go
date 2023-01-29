package fastiojoin

import (
	"github.com/plzfgme/pgsse/fastiojoin/internal/coreindex"
	"github.com/plzfgme/pgsse/fastiojoin/internal/sideindex"
)

const KeySize = coreindex.KeySize + 2*sideindex.KeySize

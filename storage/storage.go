package storage

// Retriever is the interface wraps the read methods of a kv storage.
type Retriever interface {
	// Get returns the corresponding value of the key. If the key not found, ErrKeyNotFound should be returned.
	Get(key []byte) ([]byte, error)
}

// Mutator is the interface wraps the write methods of a kv storage.
type Mutator interface {
	// Set sets the corresponding value of the key.
	Set(key, val []byte) error
	// Delete deletes the corresponding value of the key.
	Delete(key []byte) error
}

// RetrieverMutator is the interface of a kv storage.
type RetrieverMutator interface {
	Retriever
	Mutator
}

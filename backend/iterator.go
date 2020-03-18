package backend

type Iterator interface {
	SeekToFirst()
	SeekToLast()
	Seek(key []byte)
	Next()
	Prev()
	Close()
	Key() []byte
	Value() []byte
	Valid() bool
	Error() error
}

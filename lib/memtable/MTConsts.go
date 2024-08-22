package memtable

const (
	KEY_SIZE_SIZE   = 4
	VALUE_SIZE_SIZE = 4
	TOMBSTONE_SIZE  = 1

	USE_SKIP_LIST = "skip_list"
	USE_BTREE     = "btree"
	USE_MAP       = "map"
)

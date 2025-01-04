package b_tree

import (
	"fmt"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				if !ok {
					panic(fmt.Sprintf("node with ptr %d not found", ptr))
				}
				return node
			},
			new: func(node BNode) uint64 {
				if node.nbytes() > BTREE_PAGE_SIZE {
					panic(fmt.Sprintf("node size exceeds BTREE_PAGE_SIZE: %d", node.nbytes()))
				}
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				if pages[key].data != nil {
					panic(fmt.Sprintf("node with key %d already exists", key))
				}
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				if !ok {
					panic(fmt.Sprintf("node with ptr %d not found for deletion", ptr))
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}
func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

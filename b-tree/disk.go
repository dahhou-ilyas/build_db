package b_tree

import (
	"bytes"
	"fmt"
	"unsafe"
)

// C structure pour tester l'implémentation de B-Tree
type C struct {
	tree  BTree
	ref   map[string]string // Map de référence pour vérifier les résultats
	pages map[uint64]BNode  // Stock les nœuds de l'arbre
}

// newC crée une nouvelle instance de C avec des maps initialisés
func NewC() *C {
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
				// Génère une clé unique basée sur l'adresse mémoire
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				if pages[key].data != nil {
					panic(fmt.Sprintf("node with key %d already exists", key))
				}
				// Copie les données du nœud pour éviter les problèmes de référence
				nodeCopy := BNode{data: make([]byte, len(node.data))}
				copy(nodeCopy.data, node.data)
				pages[key] = nodeCopy
				return key
			},
			del: func(ptr uint64) {
				if _, ok := pages[ptr]; !ok {
					panic(fmt.Sprintf("node with ptr %d not found for deletion", ptr))
				}
				delete(pages, ptr)
			},
		},
		ref:   make(map[string]string),
		pages: pages,
	}
}

// add ajoute une paire clé-valeur à l'arbre et à la map de référence
func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

// del supprime une clé de l'arbre et de la map de référence
func (c *C) Del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

// get récupère une valeur par sa clé
func (c *C) Get(key string) (string, bool) {
	val, ok := c.tree.Get([]byte(key))
	if !ok {
		return "", false
	}
	return string(val), true
}

// verify vérifie que l'arbre et la map de référence sont cohérents
func (c *C) Verify() error {
	// Vérifie que chaque entrée dans ref existe dans l'arbre
	for key, refVal := range c.ref {
		treeVal, ok := c.tree.Get([]byte(key))
		if !ok {
			return fmt.Errorf("key %q exists in ref but not in tree", key)
		}
		if !bytes.Equal([]byte(refVal), treeVal) {
			return fmt.Errorf("value mismatch for key %q: ref=%q tree=%q",
				key, refVal, string(treeVal))
		}
	}
	return nil
}

// clear réinitialise l'état du test
func (c *C) Clear() {
	c.tree.root = 0
	c.ref = make(map[string]string)
	c.pages = make(map[uint64]BNode)
}

// size retourne le nombre d'entrées dans l'arbre
func (c *C) Size() int {
	return len(c.ref)
}

// checkNodeSize vérifie que tous les nœuds respectent la taille maximale
func (c *C) CheckNodeSize() error {
	for ptr, node := range c.pages {
		if node.nbytes() > BTREE_PAGE_SIZE {
			return fmt.Errorf("node %d exceeds max size: %d > %d",
				ptr, node.nbytes(), BTREE_PAGE_SIZE)
		}
	}
	return nil
}

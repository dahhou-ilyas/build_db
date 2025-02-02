package b_tree

import (
	"bytes"
	"encoding/binary"
)

type BNode struct {
	data []byte // can be dumped to the disk
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

// page config

const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE

	if node1max > BTREE_PAGE_SIZE {
		panic("La taille maximale du nœud dépasse la taille de la page.")
	}
}

// header

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointer

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := HEADER + 8*idx

	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	if !(1 <= idx && idx <= node.nkeys()) {
		panic("index out of bounds")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("index out of range")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	// on  mette ptr = 0 par ce que on a toujours ajouté un key/value dans leaf node donc on il y a aucun childrent pour cette raison on fait 0. c'est comme un valuer par
	// défaul qui dit il ne point sur aucun chose
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// Mettre à jour la valeur d'une clé existante dans un nœud feuille
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// Configurer l'en-tête du nouveau nœud avec le type feuille et le même nombre de clés
	new.setHeader(BNODE_LEAF, old.nkeys())

	// Copier toutes les entrées avant l'index à mettre à jour
	nodeAppendRange(new, old, 0, 0, idx)

	// Ajouter la clé mise à jour avec la nouvelle valeur
	// Le pointeur (qui est 0) reste le même que dans le nœud original
	nodeAppendKV(new, idx, old.getPtr(idx), key, val)

	// Copier toutes les entrées après l'index mis à jour
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// copy multiple KVs into the position
// a comprendre mieux
func nodeAppendRange(new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16) {
	if srcOld+n > old.nkeys() {
		panic("out of plage")
	}
	if dstNew+n > new.nkeys() {
		panic("out of plage")
	}
	if n == 0 {
		return
	}
	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)

	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
// a comprendre
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so

	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)

	default:
		panic("bad node!")
	}

	return new
}

//Handle Internal Nodes

// part of the treeInsert(): KV insertion to an internal node

func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// Split Big Nodes

// split a bigger-than-allowed node into two.
// the second node always fits on a page.

func nodeSplit2(left BNode, right BNode, old BNode) {
	// Calculate the split point that ensures the right node fits in a page
	nkeys := old.nkeys()
	var i uint16
	var subtree_size uint16

	// Find the split point by accumulating KV pairs until we reach page size
	for i = 0; i < nkeys; i++ {
		// Size of current key-value pair
		klen := uint16(len(old.getKey(i)))
		vlen := uint16(len(old.getVal(i)))
		pair_size := 4 + klen + vlen // 4 bytes for lengths + key + value

		// Size including header, pointers, and offsets
		subtree_size = HEADER + 8*(i+1) + 2*(i+1) + subtree_size + pair_size

		// Check if adding this pair would exceed page size for right node
		if subtree_size > BTREE_PAGE_SIZE {
			break
		}
	}

	// Split point found at index i
	nsplit := i

	// Configure left node
	left.setHeader(old.btype(), nsplit)
	nodeAppendRange(left, old, 0, 0, nsplit)

	// Configure right node
	right.setHeader(old.btype(), nkeys-nsplit)
	nodeAppendRange(right, old, 0, nsplit, nkeys-nsplit)

	// Validate the split
	if right.nbytes() > BTREE_PAGE_SIZE {
		panic("right node too big after split")
	}
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}

	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	if leftleft.nbytes() > BTREE_PAGE_SIZE {
		panic("node bigger than page")
	}
	return 3, [3]BNode{leftleft, middle, right}
}

//Update Internal Nodes
/*
Inserting a key into a node can result in either 1, 2 or 3 nodes. The parent node must
update itself accordingly. The code for updating an internal node is similar to that for
updating a leaf node.
*/

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// We have finished the B-tree insertion. Deletion and the rest of the code will be introduced
// in the next part

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

// nodeReplace2Kid remplace deux nœuds enfants consécutifs par un seul nœud fusionné
func nodeReplace2Kid(
	new BNode, // le nouveau nœud à construire
	old BNode, // l'ancien nœud parent
	idx uint16, // index du premier nœud à remplacer
	ptr uint64, // pointeur vers le nœud fusionné
	key []byte, // première clé du nœud fusionné
) {
	// Le nombre de clés diminue de 1 car on fusionne deux nœuds
	new.setHeader(BNODE_NODE, old.nkeys()-1)

	// Copier toutes les entrées avant les nœuds à fusionner
	nodeAppendRange(new, old, 0, 0, idx)

	// Ajouter le nouveau nœud fusionné
	// On utilise nil comme valeur car c'est un nœud interne
	nodeAppendKV(new, idx, ptr, key, nil)

	// Copier toutes les entrées après les nœuds fusionnés
	// On saute idx+2 car on remplace deux nœuds par un seul
	nodeAppendRange(
		new,                 // destination
		old,                 // source
		idx+1,               // position de destination
		idx+2,               // position source (on saute les 2 nœuds fusionnés)
		old.nkeys()-(idx+2), // nombre d'éléments restants
	)
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)
	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		if updated.nkeys() <= 0 {
			panic("updated node must have more than 0 keys")
		}
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

func (tree *BTree) Delete(key []byte) bool {
	if len(key) == 0 {
		panic("key length must not be zero")
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		panic("key length exceeds BTREE_MAX_KEY_SIZE")
	}
	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}
	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 { // remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if len(key) == 0 {
		panic("key length must not be zero")
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		panic("key length exceeds BTREE_MAX_KEY_SIZE")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		panic("value length exceeds BTREE_MAX_VAL_SIZE")
	}
	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	node := tree.get(tree.root)
	tree.del(tree.root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	// Si l'arbre est vide, retourne false
	if tree.root == 0 {
		return nil, false
	}

	// Commence à la racine
	node := tree.get(tree.root)

	// Tant qu'on n'a pas trouvé la clé ou atteint une feuille
	for {
		// Trouve l'index du plus grand enfant dont la clé est <= à la clé recherchée
		idx := nodeLookupLE(node, key)

		switch node.btype() {
		case BNODE_LEAF:
			// Dans un nœud feuille, vérifie si la clé existe
			if bytes.Equal(key, node.getKey(idx)) {
				return node.getVal(idx), true
			}
			return nil, false

		case BNODE_NODE:
			// Dans un nœud interne, descend vers l'enfant approprié
			node = tree.get(node.getPtr(idx))

		default:
			panic("bad node type")
		}
	}
}

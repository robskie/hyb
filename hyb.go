package hyb

import (
	"container/heap"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/robskie/bp128"
)

type mergeElem struct {
	ptr *[]iposting

	idx   int
	value uint32
}

// mergeHeap is a minimum heap of mergeElem.
// This is used to merge k arrays of ipostings.
type mergeHeap []mergeElem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(mergeElem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type iposting struct {
	id   uint32
	word uint32
	rank uint32
}

// Index represents a group of searchable documents.
type Index struct {
	blocks []*pblock
	words  []string

	// freqword maps the word
	// frequency (index) to word
	// id (value).
	freqword []uint32
}

// NewIndex returns an empty index.
// Call Index.Read to populate it.
func NewIndex() *Index {
	return &Index{}
}

// Write serializes the index.
func (idx *Index) Write(w io.Writer) error {
	enc := gob.NewEncoder(w)

	err := checkErr(
		enc.Encode(idx.blocks),
		enc.Encode(idx.words),
		enc.Encode(idx.freqword),
	)

	if err != nil {
		return fmt.Errorf("hyb: write failed (%v)", err)
	}

	return nil
}

// Read deserializes the index.
func (idx *Index) Read(r io.Reader) error {
	dec := gob.NewDecoder(r)

	err := checkErr(
		dec.Decode(&idx.blocks),
		dec.Decode(&idx.words),
		dec.Decode(&idx.freqword),
	)

	if err != nil {
		return fmt.Errorf("hyb: read failed (%v)", err)
	}

	return nil
}

// Search performs a search on the index given a query.
// If this search is a continuation of a previous search,
// prev should point to the previous result. This speeds
// up the search because it only needs to consider the
// documents included in the previous result.
func (idx *Index) Search(query []string, prev *Result) {
	// Check if the current query is a
	// continuation of the previous query
	cont, cquery := continuation(prev.query, query)

	// If the previous query returns no
	// results, no need to search again
	if cont && len(prev.results) == 0 {
		return
	} else if !cont {
		prev.clear()
		prev.words = idx.words
	}

	pquery := ""
	if cont {
		pquery = prev.query[len(prev.query)-1]
	}

	for _, q := range cquery {
		if len(pquery) > 0 && strings.HasPrefix(q, pquery) {
			wrange := getWordRange(q, idx.words, int(prev.wrange[0]))

			var start, end uint32
			if wrange != nil {
				start = wrange[0] - prev.wrange[0]
				end = start + (wrange[1] - wrange[0] + 1)
			}
			prev.wrange = wrange
			prev.completions = prev.completions[start:end]

			// If previous query is a prefix of the current
			// query, just filter IDs not in word range.
			prev.results = filter(prev.results, wrange)
		} else {
			idx.search(q, prev)
		}

		if len(prev.results) == 0 {
			break
		}

		pquery = q
	}

	prev.query = query
}

func (idx *Index) search(query string, prev *Result) {
	// Get blocks that contain the query
	blocks := []*pblock{}
	for _, b := range idx.blocks {
		// The first condition in the parenthesis finds the first
		// block that contains the query. The or'd condition finds
		// the succeeding blocks.
		if (b.wboundary[0] <= query && b.wboundary[1] >= query) ||
			strings.HasPrefix(b.wboundary[0], query) {

			blocks = append(blocks, b)
		}
	}

	if len(blocks) == 0 {
		prev.clear()
		return
	}

	wrange := getWordRange(query, idx.words, blocks[0].boundary[0])
	prev.wrange = wrange
	if wrange == nil {
		prev.clear()
		return
	}

	// Extend completions buffer if necessary
	rlen := int(wrange[1] - wrange[0] + 1)
	extension := rlen - len(prev.compbuf)
	if extension > 0 {
		prev.compbuf = append(prev.compbuf, make([]completion, extension)...)
	}

	// Create completions
	wid := wrange[0]
	comps := prev.compbuf[:rlen]
	for i := range comps {
		comps[i] = completion{wid, 0}
		wid++
	}

	// Intersect postings to blocks
	var posts []iposting
	postings := make([][]iposting, 0, len(blocks))
	for _, b := range blocks {
		posts, comps = intersect(prev.results, b, comps, wrange, idx.freqword)
		if len(posts) > 0 {
			postings = append(postings, posts)
		}
	}
	prev.completions = comps

	// Merge postings
	merge(&prev.results, postings)
}

func merge(results *[]iposting, posts [][]iposting) {
	// If there is only one array,
	// just copy to results.
	if len(posts) == 1 {
		*results = posts[0]
		return
	}

	// Initialize heap
	h := &mergeHeap{}
	for i, p := range posts {
		h.Push(mergeElem{&posts[i], 0, p[0].id})
	}

	// Perform k-way merge
	heap.Init(h)
	*results = (*results)[:0]
	for h.Len() > 0 {
		e := heap.Pop(h).(mergeElem)

		slice := *e.ptr
		p := slice[e.idx]
		*results = append(*results, p)

		e.idx++
		if e.idx < len(slice) {
			e.value = slice[e.idx].id
			heap.Push(h, e)
		}
	}
}

// filter removes IDs with
// words not in the word range.
func filter(posts []iposting, wrange *[2]uint32) []iposting {
	if wrange == nil {
		return nil
	}

	out := posts[:0]
	for _, p := range posts {
		if p.word >= wrange[0] && p.word <= wrange[1] {
			out = append(out, p)
		}
	}

	return out
}

func intersect(
	results []iposting,
	block *pblock,
	comps []completion,
	wrange *[2]uint32,
	freqword []uint32) ([]iposting, []completion) {

	const offset = 4
	const chunkSize = 2048
	buffer := make([]uint32, (chunkSize+offset)*3)

	ids := align(buffer[0:])
	words := align(ids[chunkSize:])
	ranks := align(words[chunkSize:])

	out := []iposting{}
	if len(results) == 0 {
		out = make([]iposting, 0, block.length)
	}

	i, j := 0, 0
	for _, p := range block.posts {
		if len(results) > 0 {
			if i >= len(results) {
				break
			} else if results[i].id > p.iboundary {
				continue
			}
		}

		bp128.Unpack(p.ids, &ids)
		bp128.Unpack(p.words, &words)
		bp128.Unpack(p.ranks, &ranks)

		var pid, pwid uint32 = math.MaxUint32, math.MaxUint32
		if len(results) > 0 {
			for j = 0; i < len(results) && j < len(ids); {
				id := results[i].id
				if id < ids[j] {
					i++
				} else if id > ids[j] {
					j++
				} else {
					wid := freqword[words[j]]
					if wid >= wrange[0] && wid <= wrange[1] {
						ip := iposting{ids[j], wid, ranks[j]}
						out = append(out, ip)

						if pid != id || pwid != wid {
							comps[wid-wrange[0]].hits++
						}

						pid = id
						pwid = wid
					}

					j++
				}
			}
		} else {
			for j, id := range ids {
				wid := freqword[words[j]]
				if wid >= wrange[0] && wid <= wrange[1] {
					ip := iposting{id, wid, ranks[j]}
					out = append(out, ip)

					if pid != id || pwid != wid {
						comps[wid-wrange[0]].hits++
					}

					pid = id
					pwid = wid
				}
			}
		}
	}

	return out, comps
}

// continuation returns true if the current
// query is a continuation of the previous query.
func continuation(prev []string, curr []string) (bool, []string) {
	if len(prev) > len(curr) {
		return false, curr
	} else if len(prev) == 0 {
		return false, curr
	}

	count := 0
	query := curr[len(prev):]
	for i := range prev {
		if strings.HasPrefix(curr[i], prev[i]) {
			if curr[i] != prev[i] {
				query = curr[i:]
			}
			count++
		} else {
			break
		}
	}

	// The current query is only a continuation
	// of the previous query if all the words in
	// the previous query is also in the current
	// query or prefix of the current query.
	if count == len(prev) {
		return true, query
	}

	return false, curr
}

func getWordRange(query string, words []string, offset int) *[2]uint32 {
	// Get the first word that is a prefix of the query
	words = words[offset:]
	rstart := sort.SearchStrings(words, query)

	// Return if no prefix found
	if rstart == len(words) || !strings.HasPrefix(words[rstart], query) {
		return nil
	}

	// Get the last prefix word
	query += string(utf8.MaxRune)
	rend := rstart + sort.SearchStrings(words[rstart:], query) - 1

	return &[2]uint32{uint32(rstart + offset), uint32(rend + offset)}
}

// align returns a 16-byte aligned subarray
// of the given array. Returns nil if it cannot
// find an aligned subarray.
func align(a []uint32) []uint32 {
	for i := range a {
		addr := unsafe.Pointer(&a[i])
		if uintptr(addr)&15 == 0 {
			return a[i:]
		}
	}

	return nil
}

func checkErr(err ...error) error {
	for _, e := range err {
		if e != nil {
			return e
		}
	}

	return nil
}

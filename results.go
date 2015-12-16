package hyb

import (
	"container/heap"
	"math"
	"sort"
)

// postHeap is a minimum heap of ipostings.
// This is used to get the top k hits.
type postHeap []iposting

func (h postHeap) Len() int           { return len(h) }
func (h postHeap) Less(i, j int) bool { return h[i].rank < h[j].rank }
func (h postHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h postHeap) Peek() iposting     { return h[0] }

func (h *postHeap) Push(x interface{}) {
	*h = append(*h, x.(iposting))
}

func (h *postHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// compHeap is a minimum heap of completions.
// This is used to get the top k completions.
type compHeap []completion

func (h compHeap) Len() int           { return len(h) }
func (h compHeap) Less(i, j int) bool { return h[i].hits < h[j].hits }
func (h compHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h compHeap) Peek() completion   { return h[0] }

func (h *compHeap) Push(x interface{}) {
	*h = append(*h, x.(completion))
}

func (h *compHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ranks []iposting

func (p ranks) Len() int           { return len(p) }
func (p ranks) Less(i, j int) bool { return p[i].rank > p[j].rank }
func (p ranks) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type completion struct {
	word uint32
	hits int
}

type byHits []completion

func (c byHits) Len() int { return len(c) }
func (c byHits) Less(i, j int) bool {
	if c[i].hits != c[j].hits {
		return c[i].hits > c[j].hits
	}

	return c[i].word < c[j].word
}
func (c byHits) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// Hits iterates over the result of a search.
type Hits struct {
	results []iposting
	current int
}

// Next increments the iterator to the next result.
// It returns false if there are no more results
// to go through.
func (h *Hits) Next() bool {
	h.current++
	if h.current >= len(h.results) {
		return false
	}
	return true
}

// ID returns the ID of the next result.
func (h *Hits) ID() int {
	return int(h.results[h.current].id)
}

// Completion represents the
// completions of the last query word.
type Completion struct {
	Word string
	Hits int
}

type hits []Completion

func (c hits) Len() int { return len(c) }
func (c hits) Less(i, j int) bool {
	if c[i].Hits != c[j].Hits {
		return c[i].Hits > c[j].Hits
	}

	return c[i].Word < c[j].Word
}
func (c hits) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// Completions iterates over the
// completions of the last query word.
type Completions struct {
	results []completion
	current int

	words []string
}

// Next increments the iterator to the next completion.
// It returns false if there are no more results to go
// through.
func (c *Completions) Next() bool {
	c.current++
	if c.current >= len(c.results) {
		return false
	}

	if c.results[c.current].hits == 0 {
		return false
	}

	return true
}

// Completion returns the next word completion.
func (c *Completions) Completion() Completion {
	res := c.results[c.current]
	return Completion{c.words[res.word], res.hits}
}

// Result contains the search result.
type Result struct {
	query   []string
	results []iposting

	words       []string
	wrange      *[2]uint32
	completions []completion
}

func (r *Result) clear() {
	r.results = nil
	r.wrange = nil
	r.completions = nil
}

// Hits returns all the IDs that match a
// given query sorted by decreasing rank.
func (r *Result) Hits() *Hits {
	cpy := make([]iposting, 0, len(r.results))

	// Remove duplicate IDs
	pid := uint32(math.MaxUint32)
	for _, p := range r.results {
		if p.id != pid {
			cpy = append(cpy, p)
			pid = p.id
		}
	}

	sort.Sort(ranks(cpy))

	return &Hits{cpy, -1}
}

// TopHits returns the top k document IDs
// that match the given query sorted by
// decreasing rank.
func (r *Result) TopHits(k int) *Hits {
	if k == 0 {
		return &Hits{nil, -1}
	}

	h := &postHeap{}
	heap.Init(h)

	pid := uint32(math.MaxUint32)
	for _, p := range r.results {
		// Remove duplicate IDs
		if p.id == pid {
			continue
		}
		pid = p.id

		if h.Len() < k {
			heap.Push(h, p)
		} else if p.rank > h.Peek().rank {
			heap.Pop(h)
			heap.Push(h, p)
		}
	}

	sort.Sort(ranks(*h))

	return &Hits{*h, -1}
}

// Completions returns all word completions of
// the last query word sorted by decreasing number
// of hits.
func (r *Result) Completions() *Completions {
	cpy := make([]completion, len(r.completions))
	copy(cpy, r.completions)
	sort.Sort(byHits(cpy))

	return &Completions{cpy, -1, r.words}
}

// TopCompletions returns the top k completions of the
// last query word sorted by decreasing number of hits.
func (r *Result) TopCompletions(k int) *Completions {
	if k == 0 {
		return &Completions{nil, -1, nil}
	}

	h := &compHeap{}
	heap.Init(h)
	for _, c := range r.completions {
		if h.Len() < k {
			heap.Push(h, c)
		} else if c.hits > h.Peek().hits {
			heap.Pop(h)
			heap.Push(h, c)
		}
	}
	sort.Sort(byHits(*h))

	return &Completions{*h, -1, r.words}
}

package hyb

import (
	"bytes"
	"encoding/gob"
	"sort"

	"github.com/robskie/bp128"
)

type doc struct {
	id    int
	words []string
	rank  int

	count   int
	deleted bool
}

type byRank []doc

func (d byRank) Len() int           { return len(d) }
func (d byRank) Less(i, j int) bool { return d[i].rank < d[j].rank }
func (d byRank) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

type byID []doc

func (d byID) Len() int { return len(d) }
func (d byID) Less(i, j int) bool {
	if d[i].id != d[j].id {
		return d[i].id < d[j].id
	}

	return d[i].count > d[j].count
}
func (d byID) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

type word struct {
	id   int
	freq int
}

type byWord []*word

func (w byWord) Len() int           { return len(w) }
func (w byWord) Less(i, j int) bool { return w[i].id < w[j].id }
func (w byWord) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

type byFrequency []*word

func (w byFrequency) Len() int           { return len(w) }
func (w byFrequency) Less(i, j int) bool { return w[i].freq > w[j].freq }
func (w byFrequency) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

type bposting struct {
	id   int
	word *string
	rank int
}

type block struct {
	posts    []*bposting
	boundary [2]int

	index  int
	length int
}

type byLen []block

func (b byLen) Len() int           { return len(b) }
func (b byLen) Less(i, j int) bool { return b[i].length > b[j].length }
func (b byLen) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type cposting struct {
	ids   *bp128.PackedInts
	words *bp128.PackedInts
	ranks *bp128.PackedInts

	iboundary uint32
}

func (p *cposting) GobEncode() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)

	err := checkErr(
		enc.Encode(p.ids),
		enc.Encode(p.words),
		enc.Encode(p.ranks),
		enc.Encode(p.iboundary),
	)

	return buf.Bytes(), err
}

func (p *cposting) GobDecode(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)

	err := checkErr(
		dec.Decode(&p.ids),
		dec.Decode(&p.words),
		dec.Decode(&p.ranks),
		dec.Decode(&p.iboundary),
	)

	return err
}

type pblock struct {
	posts  []cposting
	length int

	boundary  [2]int
	wboundary [2]string
}

func (b *pblock) GobEncode() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)

	err := checkErr(
		enc.Encode(b.posts),
		enc.Encode(b.length),
		enc.Encode(b.boundary),
		enc.Encode(b.wboundary),
	)

	return buf.Bytes(), err
}

func (b *pblock) GobDecode(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)

	err := checkErr(
		dec.Decode(&b.posts),
		dec.Decode(&b.length),
		dec.Decode(&b.boundary),
		dec.Decode(&b.wboundary),
	)

	return err
}

// Builder creates a searchable
// index from the added documents.
type Builder struct {
	docs  []doc
	count int
}

// NewBuilder creates an empty builder.
func NewBuilder() *Builder {
	return &Builder{[]doc{}, 0}
}

// Add adds a document given its ID, search keywords, and rank.
func (b *Builder) Add(id int, keywords []string, rank int) {
	sort.Strings(keywords)

	d := doc{id, keywords, rank, b.count, false}
	b.docs = append(b.docs, d)

	b.count++
}

// Delete removes a document given its ID.
func (b *Builder) Delete(id int) {
	b.docs = append(b.docs, doc{id, nil, -1, b.count, true})
	b.count++
}

// Build creates an index.
func (b *Builder) Build() *Index {
	// Normalize ranks
	sort.Sort(byRank(b.docs))
	for i := range b.docs {
		b.docs[i].rank = i
	}

	// Sort by ascending ids
	// and descending counter
	sort.Sort(byID(b.docs))

	// Remove duplicates and deleted
	// docs and create postings. Note:
	// Since docs are already sorted,
	// this results in sorted postings.
	pid := -1
	docs := b.docs[:0]
	posts := []bposting{}
	wordmap := map[string]*word{}
	for _, d := range b.docs {
		if pid != d.id && !d.deleted {
			// Create postings
			for j := range d.words {
				w := d.words[j]

				if wf := wordmap[w]; wf != nil {
					wf.freq++
				} else {
					wordmap[w] = &word{-1, 1}
				}

				p := bposting{d.id, &d.words[j], d.rank}
				posts = append(posts, p)

			}

			docs = append(docs, d)
		}
		pid = d.id
	}
	b.docs = docs

	// Return empty index if no postings
	if len(posts) == 0 {
		return &Index{}
	}

	// Create words array and sort in lexicographical order
	nchars := 0
	wordcount := 0
	words := make([]string, 0, len(wordmap))
	for word, wf := range wordmap {
		words = append(words, word)

		nchars += len(word)
		wordcount += wf.freq
	}
	sort.Strings(words)

	// Create word-frequency array
	wfreqs := make([]*word, len(words))
	for i, w := range words {
		word := wordmap[w]
		word.id = i

		wfreqs[i] = word
	}

	// Create frequency-word map
	sort.Sort(byFrequency(wfreqs))
	freqs := make([]int, len(words))
	freqword := make([]uint32, len(words))
	for i, wf := range wfreqs {
		freqword[i] = uint32(wf.id)
		freqs[wf.id] = wf.freq

		wf.freq = i
	}

	// Create character frequency array
	cavg := ((nchars / len(words)) / 2) + 1
	charfreq := getCharFreq(words, freqs, cavg)

	// Create blocks
	const nblocks = 5
	blockSize := (wordcount / nblocks) + 1
	blocks, wordBlock := createBlocks(
		nblocks,
		blockSize,
		words,
		freqs,
		cavg,
	)

	// Put postings to blocks
	for i, p := range posts {
		widx := wordmap[*p.word].id
		bidx := wordBlock(widx)
		blocks[bidx].posts = append(blocks[bidx].posts, &posts[i])
	}

	// Compress blocks
	pblocks := make([]*pblock, len(blocks))
	for i, blk := range blocks {

		// Create arrays for packing
		pids := make([]uint32, blk.length)
		pwords := make([]uint32, blk.length)
		pranks := make([]uint32, blk.length)
		for j, p := range blk.posts {
			pids[j] = uint32(p.id)
			pranks[j] = uint32(p.rank)
			pwords[j] = uint32(wordmap[*p.word].freq)
		}

		const chunkSize = 2048
		nchunks := blk.length / chunkSize
		if blk.length%chunkSize > 0 {
			nchunks++
		}

		posts := make([]cposting, nchunks)
		for k := range posts {
			start := k * chunkSize
			end := min(start+chunkSize, blk.length)

			posts[k].ids = bp128.DeltaPack(pids[start:end])
			posts[k].words = bp128.Pack(pwords[start:end])
			posts[k].ranks = bp128.Pack(pranks[start:end])

			posts[k].iboundary = pids[end-1]
		}

		// Create packed block
		pb := &pblock{}
		pb.posts = posts
		pb.length = blk.length
		pb.boundary = blk.boundary
		pb.wboundary = [2]string{
			words[blk.boundary[0]],
			words[blk.boundary[1]],
		}

		pblocks[i] = pb
	}

	return &Index{
		blocks:   pblocks,
		words:    words,
		freqword: freqword,
		charfreq: charfreq,
	}
}

// createBlocks creates blocks by grouping words
// with the same prefix. This is done to minimize
// merging when searching.
func createBlocks(
	nblocks int,
	blockSize int,
	words []string,
	freqs []int,
	cavg int) ([]block, func(int) int) {

	sum := make([]int, cavg)
	start := make([]int, cavg)
	blocks := make([][]block, cavg)

	prev := 0
	for i, w := range words {
		prefix := true
		pw := words[prev]
		minlen := min(len(pw), len(w))
		for j := range blocks {
			if !prefix || j >= minlen || pw[j] != w[j] {
				prefix = false
			}

			if sum[j] >= blockSize && !prefix {
				b := block{}
				b.length = sum[j]
				b.boundary[0] = start[j]
				b.boundary[1] = prev
				blocks[j] = append(blocks[j], b)

				sum[j] = 0
				start[j] = i
			}

			sum[j] += freqs[i]
		}

		prev = i
	}

	// Process the last block
	lastw := len(words) - 1
	for j := range blocks {
		b := block{}
		b.length = sum[j]
		b.boundary[0] = start[j]
		b.boundary[1] = lastw
		blocks[j] = append(blocks[j], b)
	}

	blk := blocks[0]
	for _, b := range blocks {
		if len(b) >= nblocks {
			blk = b
			break
		}
	}

	cblk := make([]block, len(blk))
	copy(cblk, blk)
	for i := range cblk {
		cblk[i].index = i
	}

	sort.Sort(byLen(cblk))
	mapFunc := func(w int) int {
		for _, b := range cblk {
			if w >= b.boundary[0] && w <= b.boundary[1] {
				return b.index
			}
		}

		// Should not reach here
		return -1
	}

	return blk, mapFunc
}

// getCharFreq returns a [x][y]uint32 array (cfreq) which contains
// the maximum frequency of a character (x) given its position (y)
// taking into account the frequency of the previous character.
// For example, given the word-frequency pairs:
// aab - 1
// abc - 2
// bbc - 3
// ddc - 4
// cfreq['b'][1] = max(freq(abc), freq(bbc)) = 3
// cfreq['c'][2] = max(freq(abc) + freq(bbc), freq(ddc)) = 5
func getCharFreq(words []string, freqs []int, cavg int) [][]uint32 {
	cfreq := make([][]uint32, 256)
	for i := range cfreq {
		cfreq[i] = make([]uint32, cavg)
	}

	// For cfreq[x][0] (first word position),
	// just add all the occurrences of the words
	// that have their first character set to x.
	for i, w := range words {
		cfreq[w[0]][0] += uint32(freqs[i])
	}

	// For the succeeding word positions, assume that
	// S is the set of all characters that precedes the
	// character at position i, and x is the character at
	// position i. Then cfreq[x][i] is the maximum frequency
	// among all the frequencies of characters in S.
	for i := 1; i < cavg; i++ {
		ctmp := [256][256]int{}

		for j, w := range words {
			if i < len(w) {
				ctmp[w[i]][w[i-1]] += freqs[j]
			}
		}

		for j := range ctmp {
			max := 0
			for _, v := range ctmp[j] {
				if v > max {
					max = v
				}
			}

			cfreq[j][i] = uint32(max)
		}
	}

	return cfreq
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

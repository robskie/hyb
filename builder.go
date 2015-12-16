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

	length int
}

type pblock struct {
	ids   *bp128.PackedInts
	words *bp128.PackedInts
	ranks *bp128.PackedInts

	boundary  [2]int
	wboundary [2]string
}

func (b *pblock) GobEncode() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)

	err := checkErr(
		enc.Encode(b.ids),
		enc.Encode(b.words),
		enc.Encode(b.ranks),
		enc.Encode(b.boundary),
		enc.Encode(b.wboundary),
	)

	return buf.Bytes(), err
}

func (b *pblock) GobDecode(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)

	err := checkErr(
		dec.Decode(&b.ids),
		dec.Decode(&b.words),
		dec.Decode(&b.ranks),
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

	// Create words array and sort in lexicographical order
	wordcount := 0
	words := make([]string, 0, len(wordmap))
	for word, wf := range wordmap {
		words = append(words, word)
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

	// Create blocks
	const blockCount = 5
	blocks := []block{}
	nblock, sum := 0, 0
	wstart, wend := 0, 0
	blockSize := (wordcount / blockCount) + 1
	wordblock := make([]int, len(wfreqs))
	for i, freq := range freqs {
		wend = i
		wordblock[i] = nblock
		sum += freq

		if sum >= blockSize || i == len(wfreqs)-1 {
			b := block{}
			b.boundary[0] = wstart
			b.boundary[1] = wend
			blocks = append(blocks, b)

			sum = 0
			nblock++
			wstart = i + 1
		}
	}

	// Put postings to blocks
	for i, p := range posts {
		widx := wordmap[*p.word].id
		bidx := wordblock[widx]
		blocks[bidx].posts = append(blocks[bidx].posts, &posts[i])
	}

	// Compress blocks
	pblocks := make([]*pblock, len(blocks))
	for i, blk := range blocks {

		// Create arrays for packing
		pids := make([]uint32, len(blk.posts))
		pwords := make([]uint32, len(blk.posts))
		pranks := make([]uint32, len(blk.posts))
		for i, p := range blk.posts {
			pids[i] = uint32(p.id)
			pranks[i] = uint32(p.rank)
			pwords[i] = uint32(wordmap[*p.word].freq)
		}

		// Create packed block
		pb := &pblock{}
		pb.ids = bp128.DeltaPack(pids)
		pb.words = bp128.Pack(pwords)
		pb.ranks = bp128.Pack(pranks)
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
	}
}

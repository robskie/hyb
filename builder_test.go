package hyb

import (
	"bufio"
	"compress/gzip"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/robskie/bp128"
	"github.com/stretchr/testify/assert"
)

func TestBuildEmpty(t *testing.T) {
	defer func() {
		assert.Nil(t, recover())
	}()

	builder := NewBuilder()
	index := builder.Build()
	assert.NotNil(t, index)
}

type tdoc struct {
	id    int
	words []string
	rank  int
}

func TestBuild(t *testing.T) {
	file, err := os.Open("files/books.txt.gz")
	assert.Nil(t, err)
	defer file.Close()

	fgzip, err := gzip.NewReader(file)
	assert.Nil(t, err)
	defer fgzip.Close()

	mapidx := map[int]tdoc{}
	scanner := bufio.NewScanner(fgzip)
	for i := 0; scanner.Scan(); i++ {
		words := strings.Fields(strings.ToLower(scanner.Text()))
		sort.Strings(words)
		mapidx[i] = tdoc{i, words, i}
	}

	b := NewBuilder()

	// Delete items that weren't added
	for i := 0; i < 100; i++ {
		b.Delete(i)
	}

	// Add items
	for id, doc := range mapidx {
		b.Add(id, doc.words, doc.rank)
	}

	// Overwrite items
	w := []string{
		"the",
		"answer",
		"to",
		"life",
		"the",
		"universe",
		"and",
		"everything",
	}
	sort.Strings(w)
	mapidx[42] = tdoc{42, w, 42}
	b.Add(42, w, 42)

	// Delete some items
	for i := 50; i < 100; i++ {
		delete(mapidx, i)
		b.Delete(i)
	}

	index := b.Build()
	pindex := parseIndex(index)

	assert.Equal(t, len(mapidx), len(pindex))
	for id, doc := range mapidx {
		if !assert.Equal(t, doc.words, pindex[id].words) {
			break
		}
	}
}

func parseIndex(index *Index) map[int]tdoc {
	ids := []uint32{}
	words := []uint32{}
	ranks := []uint32{}
	mapidx := map[int]tdoc{}
	for _, b := range index.blocks {
		for _, p := range b.posts {
			bp128.Unpack(p.ids, &ids)
			bp128.Unpack(p.words, &words)
			bp128.Unpack(p.ranks, &ranks)

			for i, id := range ids {
				word := index.words[index.freqword[words[i]]]

				doc := mapidx[int(id)]
				doc.id = int(id)
				doc.words = append(doc.words, word)
				doc.rank = int(ranks[i])
				sort.Strings(doc.words)

				mapidx[int(id)] = doc
			}
		}
	}

	return mapidx
}

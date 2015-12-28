package hyb

import (
	"bufio"
	"compress/gzip"
	"os"
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

func TestBuild(t *testing.T) {
	file, err := os.Open("files/books.txt.gz")
	assert.Nil(t, err)
	defer file.Close()

	fgzip, err := gzip.NewReader(file)
	assert.Nil(t, err)
	defer fgzip.Close()

	idwords := map[int][]string{}
	scanner := bufio.NewScanner(fgzip)
	for i := 0; scanner.Scan(); i++ {
		words := strings.Fields(strings.ToLower(scanner.Text()))
		idwords[i] = words
	}

	b := NewBuilder()

	// Delete items that weren't added
	for i := 0; i < 100; i++ {
		b.Delete(i)
	}

	// Add items
	for id, words := range idwords {
		b.Add(id, words, 0)
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
	idwords[42] = w
	b.Add(42, w, 0)

	// Delete some items
	for i := 50; i < 100; i++ {
		delete(idwords, i)
		b.Delete(i)
	}

	index := b.Build()
	nidwords := parseIndex(index)
	assert.Equal(t, len(idwords), len(nidwords))

Exit:
	for id, words := range idwords {
		nwords, ok := nidwords[id]

		if !assert.True(t, ok) {
			break
		}

		if !assert.Equal(t, len(words), len(nwords)) {
			break
		}

		for _, w := range nwords {
			if !assert.Contains(t, words, w) {
				break Exit
			}
		}
	}
}

// parseIndex transforms the given index to
// a map with document id as key and document
// keywords as value.
func parseIndex(index *Index) map[int][]string {
	ids := []uint32{}
	words := []uint32{}
	idwords := map[int][]string{}
	for _, b := range index.blocks {
		for _, p := range b.posts {
			bp128.Unpack(p.ids, &ids)
			bp128.Unpack(p.words, &words)

			for i, id := range ids {
				word := index.words[index.freqword[words[i]]]
				wslice := idwords[int(id)]
				wslice = append(wslice, word)
				idwords[int(id)] = wslice
			}
		}
	}

	return idwords
}

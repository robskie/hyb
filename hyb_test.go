package hyb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	idsA := []int{0, 0, 1, 5, 7}
	idsB := []int{2, 4, 6, 7, 8, 9}
	idsC := []int{3, 5, 7, 10}
	ids := [][]int{idsA, idsB, idsC}

	out := []uint32{0, 0, 1, 2, 3, 4, 5, 5, 6, 7, 7, 7, 8, 9, 10}

	postsA := make([]iposting, len(idsA))
	postsB := make([]iposting, len(idsB))
	postsC := make([]iposting, len(idsC))
	posts := [][]iposting{postsA, postsB, postsC}

	for i, a := range ids {
		for j, v := range a {
			posts[i][j] = iposting{uint32(v), 0, 0}
		}
	}

	res := []iposting{}
	merge(&res, posts)
	for i, v := range res {
		if !assert.Equal(t, out[i], v.id) {
			break
		}
	}
}

func TestContinuation(t *testing.T) {
	_, docs := createIndex("files/books.txt.gz")

Exit:
	for _, d := range docs {
		prev := []string{string(d[0])}
		for _, sub := range substrings(d) {
			curr := strings.Fields(sub)

			cont, query := continuation(prev, curr)
			if !assert.True(t, cont) {
				break Exit
			}

			if len(query) > 0 {
				if !assert.Equal(t, curr[len(curr)-1], query[len(query)-1]) {
					break Exit
				}
			}

			prev = curr
		}
	}
}

func TestIndexEmpty(t *testing.T) {
	defer func() {
		assert.Nil(t, recover())
	}()

	builder := NewBuilder()
	index := builder.Build()
	index.Search([]string{"abc"}, &Result{})
}

func TestIndexSearch(t *testing.T) {
	index, docs := createIndex("files/books.txt.gz")

	res := &Result{}
	sdocs := make([][]string, len(docs))
	for i, d := range docs {
		sdocs[i] = strings.Fields(d)
	}

Exit:
	for _, d := range docs {
		for _, sub := range substrings(d) {
			query := strings.Fields(sub)

			index.Search(query, res)
			hits, completions := search(sdocs, query)

			ith := res.Hits()
			actualHits := []int{}
			for ith.Next() {
				actualHits = append(actualHits, ith.ID())
			}

			ith = res.TopHits(len(hits))
			topHits := make([]int, 0, len(hits))
			for ith.Next() {
				topHits = append(topHits, ith.ID())
			}

			itc := res.Completions()
			actualComps := []Completion{}
			for itc.Next() {
				actualComps = append(actualComps, itc.Completion())
			}

			itc = res.TopCompletions(len(completions))
			topComps := make([]Completion, 0, len(completions))
			for itc.Next() {
				topComps = append(topComps, itc.Completion())
			}

			if !assert.Equal(t, hits, actualHits) {
				break Exit
			}
			if !assert.Equal(t, hits, topHits) {
				break Exit
			}
			if !assert.Equal(t, completions, actualComps) {
				break Exit
			}
			if !assert.Equal(t, completions, topComps) {
				break Exit
			}
		}
	}
}

func TestIndexWriteRead(t *testing.T) {
	b := NewBuilder()
	b.Add(0, []string{"ab", "bc", "cd"}, 0)
	idx := b.Build()

	buf := &bytes.Buffer{}
	err := idx.Write(buf)
	assert.Nil(t, err)

	nidx := NewIndex()
	err = nidx.Read(buf)
	assert.Nil(t, err)

	assert.Equal(t, idx, nidx)
}

func BenchmarkIndexSearch(b *testing.B) {
	queries := make([][]string, 0, b.N)
	index, docs := createIndex("files/movies.txt.gz")

	qcount := 0
	for qcount < b.N {
		doc := docs[rand.Intn(len(docs))]
		subs := substrings(doc)

		for _, s := range subs {
			queries = append(queries, strings.Fields(s))
			qcount++
		}
	}

	b.ResetTimer()
	res := &Result{}
	for i := 0; i < b.N; i++ {
		index.Search(queries[i], res)
	}
}

var createIndex = func() func(string) (*Index, []string) {
	var f string
	var index *Index
	var docs []string

	return func(filename string) (*Index, []string) {
		if f != filename {
			f = filename
			docs = []string{}

			file, err := os.Open(f)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			fgzip, err := gzip.NewReader(file)
			if err != nil {
				panic(err)
			}
			defer fgzip.Close()

			scanner := bufio.NewScanner(fgzip)

			b := NewBuilder()
			rank := math.MaxUint32
			for i := 0; scanner.Scan(); i++ {
				doc := strings.ToLower(scanner.Text())
				words := strings.Fields(doc)

				docs = append(docs, doc)
				b.Add(i, words, rank)

				rank--
			}

			index = b.Build()
		}

		return index, docs
	}
}()

// search performs a brute-force search for matching documents
// given a query string. It also returns the completions of the
// last query word that has at least one matching document.
func search(docs [][]string, query []string) ([]int, []Completion) {
	results := []int{}
	comps := map[string]int{}
	lastq := query[len(query)-1]
	for i, d := range docs {
		if matches(query, d) {
			results = append(results, i)

			p := prefixes(lastq, d)
			for _, w := range p {
				comps[w]++
			}
		}

	}

	c := make([]Completion, 0, len(comps))
	for word, hits := range comps {
		c = append(c, Completion{word, hits})
	}
	sort.Sort(hits(c))

	return results, c
}

// prefixes returns all the strings in words
// that is a prefix of the query string.
func prefixes(query string, words []string) []string {
	sort.Strings(words)

	prev := ""
	res := []string{}
	for _, w := range words {
		if strings.HasPrefix(w, query) && prev != w {
			res = append(res, w)
		}
		prev = w
	}

	return res
}

// matches returns true if all the words in
// query are prefixes of the strings in words.
func matches(query []string, words []string) bool {
	if len(query) == 0 {
		return false
	}

	count := 0
	for _, q := range query {
		for _, w := range words {
			if strings.HasPrefix(w, q) {
				count++
				break
			}
		}
	}

	if count == len(query) {
		return true
	}

	return false
}

// substrings returns a set of
// prefixes of the given string.
//
// Input: "abc" Output: "a", "ab", "abc"
func substrings(line string) []string {
	runes := []rune(line)
	sub := make([]string, 0, len(runes))
	for i, r := range runes {
		if unicode.IsSpace(r) {
			continue
		}

		sub = append(sub, string(runes[:i+1]))
	}

	return sub
}

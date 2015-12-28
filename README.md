# hyb

Package hyb implements the HYB structure described in [Type Less, Find
More: Fast Autocompletion Search with a Succinct Index][1] by Holger Bast and
Ingmar Weber. It provides an index which gives the word completions of the last
query word and returns the best hits for any of those completions.

[1]: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.453.9716&rep=rep1&type=pdf

## Requirements

This package uses [SIMD-BP128][2] integer encoding and decoding, so it requires
an x86_64/AMD64 CPU that supports SSE2 instructions.


[2]: http://arxiv.org/abs/1209.2137

## Installation
```sh
go get github.com/robskie/hyb
```

## Usage

```go

// Sample document structure
type Doc struct {
	ID       int
	Keywords []string
	Rank     int
}

// Create a builder and add documents to it
builder := hyb.NewBuilder()
for _, d := range docs {
  builder.Add(d.ID, d.Keywords, d.Rank)
}

// Build the index
index := builder.Build()

// Search the index
result := &hyb.Result{}
query := strings.Fields("abc")
index.Search(query, result)

// Get the top 10 hits
hits := result.TopHits(10)
for hits.Next() {
  id := hits.ID()
}

// Get the top 5 completions
comps := result.TopCompletions(5)
for comps.Next() {
  c := comps.Completion()
}

```

## API Reference

Godoc documentation can be found [here][3].

[3]: https://godoc.org/github.com/robskie/hyb

## Benchmarks

The machine used in this benchmark is a Core i5 at 2.3GHz. The test index
contains 1159741 postings taken from 357007 movie titles. The benchmark is done
by choosing a random movie and taking all its prefixes and using those as query
parameters. For example, given the movie, "Alien", the resulting queries would
be "A", "Al", "Ali", "Alie", and "Alien" in that order.

You can run this benchmark on your machine by typing this command
```go test github.com/robskie/hyb -bench=.*``` in terminal.

```
BenchmarkIndexSearch-4	    5000	   747618 ns/op
```

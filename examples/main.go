package main

import (
	"io"
	"os"

	"github.com/AumSahayata/cdcgo/chunk"
	"github.com/AumSahayata/cdcgo/fastcdc"
	"github.com/AumSahayata/cdcgo/index"
	"github.com/AumSahayata/cdcgo/manifest"
	"github.com/AumSahayata/cdcgo/storage"
)

func main() {
	file := "benchmark/testdata/linuxmint-22.1-cinnamon-64bit.iso"
	stat, _ := os.Stat(file)
	data, _ := os.Open(file)
	defer data.Close()
	root := "chunks"
	hashAlgo := "sha256"

	p := fastcdc.NewParams(1*1024*1024, 4*1024*1024, 8*1024*1024, nil)
	chunker := fastcdc.NewChunker(&p)

	idx, _ := index.NewPersistentIndexJSON(root + "/index.json")
	m := manifest.NewManifest("test.iso", stat.Size(), hashAlgo)

	cr, _ := chunk.NewChunkReader(data, hashAlgo, 8*1024*1024, chunker)
	fs, _ := storage.NewFSStorage(root, idx)

	for {
		ch, d, err := cr.Next()
		if err == io.EOF {
			break
		}

		fs.Save(ch, d)
		m.Chunks = append(m.Chunks, ch)
	}

	m.Save(root + "/manifest.json")
}

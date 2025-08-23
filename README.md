# 📘 cdcgo – Content-Defined Chunking in Go (Pre-Release)

**cdcgo** is a Go library that implements [FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf), 
a high-performance **content-defined chunking (CDC)** algorithm for deduplication and data storage.

[![Go Reference](https://pkg.go.dev/badge/github.com/AumSahayata/cdcgo.svg)](https://pkg.go.dev/github.com/AumSahayata/cdcgo)

---

## ✨ Why cdcgo?
Traditional fixed-size chunking wastes space when data shifts.  
Content-defined chunking (CDC) finds **natural data boundaries**, enabling efficient deduplication — ideal for:
- Backups & snapshots  
- Object storage (S3/MinIO)  
- Large-file synchronization  

---

## 🚀 Core Features (MVP)
- Idiomatic Go API for FastCDC  
- Streaming support (`io.Reader` / `io.Writer`) — scales to GB-size files  
- Chunk metadata (offset, size, SHA-256 hash)  
- Benchmarks + tests  
- Examples: split & reassemble files  

---

## 📍 Roadmap
- Dedupe helpers (chunk indexing)  
- Storage backends: local FS + S3/MinIO  
- CLI tool (`cdcbench`) for benchmarking & stats  
- Configurable hash functions (SHA-1, SHA-256, BLAKE3)  

---

## 📦 Installation
```bash
go get github.com/AumSahayata/cdcgo
```

## 📖 Documentation

- [pkg.go.dev documentation](https://pkg.go.dev/github.com/AumSahayata/cdcgo)  
- Examples available in the `examples/` folder (Soon!)

---

## 📝 License

This project is licensed under the MIT License. See [LICENSE](https://github.com/AumSahayata/cdcgo/blob/main/LICENSE) for details.

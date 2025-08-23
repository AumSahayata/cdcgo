# 📘 cdcgo – Content-Defined Chunking in Go (ALPHA)

**cdcgo** is a Go library that implements [FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf),  
a high-performance **content-defined chunking (CDC)** algorithm for deduplication and data storage.

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

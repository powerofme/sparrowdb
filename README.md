# sparrowDb

**sparrowDb** is a high-performance, in-memory columnar database and analytical query engine built for **.NET 10**, leveraging native **DuckDB** execution and **Apache Arrow IPC** streaming.

Designed for high-throughput, low-latency analytical workloads, sparrowDb operates **100% in-memory**, eliminating local disk dependencies and housekeeping overheads.

---

## ⚡ Key Features

- **100% In-Memory Columnar Engine**: Runs embedded DuckDB instances directly in RAM. Zero temporary disk files, zero housekeeping requirements.
- **Native Vectorized Arrow IPC Ingestion**: Ingests Apache Arrow IPC streams directly into DuckDB using the Arrow C Data Interface (`duckdb_arrow_scan`), bypassing managed row-by-row iteration and achieving up to **25,000,000+ rows/sec** with zero GC allocations.
- **Multi-Worker Concurrency**: Multiple queue workers concurrently ingesting into distinct tables on a shared `:memory:` database with zero lock contention (**76,000,000+ rows/sec** across 23 concurrent tables).
- **In-Memory Parquet Byte Buffers & Chunking**: Export and ingest Parquet directly as `byte[]` or chunked `IEnumerable<byte[]>` without local disk persistence—ideal for custom HTTP connection pooling and parallel blob loading with IBM COS or S3.
- **Vectorized Analytical SQL**: Native SIMD-accelerated filtering, aggregation, and pivoting over millions of records in milliseconds.
- **Enterprise Object Storage**: Stream data directly to and from S3-compatible cloud object storage without touching local disk.

---

## 🏎️ Ingestion Architecture: Native Arrow IPC vs Legacy Scalar

### Native Vectorized Path (`IngestArrowIpcNative`)
```
Python (PyArrow) / .NET
         ↓
  Arrow IPC stream
         ↓
    byte[] / ReadOnlyMemory<byte>
         ↓
NativeArrowStreamHolder (Arrow C Data Interface)
         ↓
DuckDB Vectorized Execution (duckdb_arrow_scan)
         ↓
INSERT INTO target_table SELECT * FROM arrow_source
```
- **Zero row-by-row managed iteration**.
- **Zero scalar P/Invoke calls** (no `duckdb_append_*`).
- **Zero buffer copies**: memory buffers pinned in-place for DuckDB SIMD vector scanning.
- **100% In-Memory**: No temporary disk files.

### Public API Usage

```csharp
using SparrowDb;

// Initialize in-memory database
using var db = new Database(":memory:");

// Ingest Arrow IPC bytes into a table (creates table if not exists)
db.IngestArrowIpcNative("trades", arrowIpcBytes);

// For multi-worker scenarios, use worker connections:
using var workerConn = db.CreateConnection();
workerConn.IngestArrowIpcNative("worker_table", arrowIpcBytes);

// Run vectorized analytical query
using var result = db.ExecuteQuery("SELECT category, count(*), sum(amount) FROM trades GROUP BY category;");
```

### Performance Benchmarks (Apple M3 Max)

#### Ingestion Path Comparison
| Rows | Path | Time (ms) | Throughput (rows/s) | GC0 / GC1 / GC2 | Allocated MB |
|:-----|:-----|:----------|:--------------------|:----------------|:-------------|
| 100,000 | Legacy Appender | 51 ms | 1,960,784 | 2 / 1 / 1 | 9.6 MB |
| 100,000 | **Native Arrow IPC** | **14 ms** | **7,142,857** | **1 / 1 / 1** | **0.0 MB** |
| 500,000 | Legacy Appender | 179 ms | 2,793,296 | 7 / 1 / 1 | 48.1 MB |
| 500,000 | **Native Arrow IPC** | **20 ms** | **25,000,000** | **1 / 1 / 1** | **0.0 MB** |

#### Multi-Worker Concurrency (1 Database `:memory:`, N Connections, N Tables)
| Workers / Tables | Total Rows | Total Data | Elapsed | Aggregated Throughput | Status |
|:-----------------|:-----------|:-----------|:--------|:----------------------|:-------|
| 1 | 50,000 | 2.5 MB | 5 ms | 10,000,000 rows/s | PASSED |
| 4 | 200,000 | 10.0 MB | 5 ms | 40,000,000 rows/s | PASSED |
| 8 | 400,000 | 20.0 MB | 7 ms | 57,142,857 rows/s | PASSED |
| 16 | 800,000 | 40.1 MB | 15 ms | 53,333,333 rows/s | PASSED |
| **23** | **1,150,000** | **57.6 MB** | **15 ms** | **76,666,667 rows/s** | **PASSED** |

#### Wide Dataset Stress Test (1,000,000 Rows x 100 Columns, 60 Strings, 1 Single File)
| Metric | Value |
|:---|:---|
| **Total Ingested Rows** | **1,000,000** |
| **Total Columns** | **100** (60 String, 20 Int32, 10 Int64, 5 Double, 5 Timestamp) |
| **Single File Size** | **729.66 MB** (765,105,816 bytes) |
| **Ingestion Time** | **1,194 ms (1.19 seconds)** |
| **Ingestion Throughput** | **837,521 rows/sec (611.1 MB/sec)** |
| **Verification** | `COUNT(*) = 1,000,000` (**PASSED**) |

---

## 📂 Repository Structure

```
sparrowDb/
├── src/                                  # Core engine library
│   ├── Arrow/                            # Apache Arrow IPC & Native Stream Holder
│   │   ├── NativeArrowIpcReader.cs       # Native Arrow IPC ingestion engine
│   │   ├── NativeArrowStreamHolder.cs    # C Data Interface ArrowArrayStream bridge
│   │   └── ArrowBatchReader.cs           # Legacy scalar stream reader (compatibility)
│   ├── Native/                           # DuckDB native bindings & safe handles
│   ├── Parquet/                          # In-memory Parquet buffer export & ingestion
│   ├── S3/                               # S3 storage connector
│   ├── Connection.cs                     # Connection wrapper & query execution
│   └── Database.cs                       # In-memory database manager
│
└── tests/
    ├── sparrowDb.Tests/                  # Comprehensive unit & integration tests
    └── sparrowDb.BenchmarkRunner/        # Benchmarks comparing paths & concurrency
```

---

## 🚀 Getting Started

### Prerequisites
- [.NET 10 SDK](https://dotnet.microsoft.com/)
- Supported OS: macOS (ARM64 / x64), Linux (x64 / ARM64), Windows (x64 / ARM64)

### Building the Solution
```bash
dotnet build sparrowDb.sln -c Release
```

### Running Unit Tests
```bash
dotnet test
```

### Running the Benchmark Suite
```bash
dotnet run --project tests/sparrowDb.BenchmarkRunner/sparrowDb.BenchmarkRunner.csproj -c Release
```

---

## 📄 License
MIT License.

# sparrowDb

**sparrowDb** is a high-performance, in-memory columnar database and analytical query engine built for **.NET 10**, leveraging native **DuckDB** execution and **Apache Arrow IPC** streaming.

Designed for high-throughput, low-latency analytical workloads, sparrowDb operates **100% in-memory**, eliminating local disk dependencies and housekeeping overheads.

---

## ⚡ Key Features

- **100% In-Memory Columnar Engine**: Runs embedded DuckDB instances directly in RAM. Zero temporary disk files, zero housekeeping requirements.
- **Apache Arrow IPC Streaming**: Ingest Apache Arrow IPC byte streams directly into columnar tables at memory bandwidth speeds.
- **In-Memory Parquet Byte Buffers & Chunking**: Export and ingest Parquet directly as `byte[]` or chunked `IEnumerable<byte[]>` without local disk persistence—ideal for custom HTTP connection pooling and parallel blob loading with IBM COS or S3.
- **Vectorized Analytical SQL**: Native SIMD-accelerated filtering, aggregation, and pivoting over millions of records in milliseconds.
- **Enterprise Object Storage**: Stream data directly to and from S3-compatible cloud object storage without touching local disk.

---

## 📂 Repository Structure

```
sparrowDb/
├── src/                                  # Core engine library
│   ├── Arrow/                            # Apache Arrow IPC stream ingestion
│   ├── Native/                           # DuckDB native bindings & safe handles
│   ├── S3/                               # S3 storage connector
│   ├── Connection.cs                     # Connection wrapper & query execution
│   └── Database.cs                       # In-memory database manager
│
└── tests/
    ├── sparrowDb.Tests/                  # Unit & integration test suite (xUnit)
    └── sparrowDb.BenchmarkRunner/        # Standalone multi-tenant benchmark runner
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
dotnet test tests/sparrowDb.Tests/sparrowDb.Tests.csproj
```

### Running the Multi-Tenant Benchmark
```bash
dotnet run --project tests/sparrowDb.BenchmarkRunner/sparrowDb.BenchmarkRunner.csproj -c Release
```

---

## 📄 License
MIT License.

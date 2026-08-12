```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.26200.8875)
Intel Core i7-10510U CPU 1.80GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 10.0.201
  [Host] : .NET 10.0.5 (10.0.526.15411), X64 RyuJIT AVX2

Job=InProcess  Toolchain=InProcessEmitToolchain  

```
| Method                                                | Mean        | Error      | StdDev      | Median      | Allocated |
|------------------------------------------------------ |------------:|-----------:|------------:|------------:|----------:|
| &#39;Step 1: Slab Creation &amp; Merkle Hash (Rust FFI)&#39;      | 935.3547 ns | 48.1890 ns | 137.4859 ns | 905.2896 ns |         - |
| &#39;Step 2: Bitmask Step Mark &amp; Transition (Rust FFI)&#39;   | 998.4076 ns | 48.2845 ns | 138.5374 ns | 991.2462 ns |         - |
| &#39;Step 3: Merkle Root SHA-256 Verification (Rust FFI)&#39; | 963.4448 ns | 41.4251 ns | 122.1429 ns | 950.9992 ns |         - |
| &#39;Step 4: NDA Binary Document Proof Verification&#39;      | 473.3349 ns | 22.9638 ns |  64.7699 ns | 468.1457 ns |         - |
| &#39;Step 5: VCTP Packet Header Construction&#39;             |  13.8591 ns |  1.0809 ns |   3.1012 ns |  13.2363 ns |         - |
| &#39;Step 6: Tier-2 Bump Arena Payload Allocation&#39;        |  11.6395 ns |  0.6761 ns |   1.9723 ns |  11.5844 ns |         - |
| &#39;Step 7: O(1) Direct Memory Pointer Resumption&#39;       |   0.0157 ns |  0.0253 ns |   0.0723 ns |   0.0000 ns |         - |

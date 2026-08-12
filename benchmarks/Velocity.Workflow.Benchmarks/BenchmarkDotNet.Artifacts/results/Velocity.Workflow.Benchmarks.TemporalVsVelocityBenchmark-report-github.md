```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.26200.8875)
Intel Core i7-10510U CPU 1.80GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 10.0.201
  [Host] : .NET 10.0.5 (10.0.526.15411), X64 RyuJIT AVX2

Job=InProcess  Toolchain=InProcessEmitToolchain  

```
| Method                                                                   | StepCount | Mean               | Error             | StdDev             | Median             | Ratio | RatioSD | Gen0     | Allocated | Alloc Ratio |
|------------------------------------------------------------------------- |---------- |-------------------:|------------------:|-------------------:|-------------------:|------:|--------:|---------:|----------:|------------:|
| **&#39;Traditional Temporal: Full Event History Deserialization &amp; Replay Loop&#39;** | **10**        |     **32,187.0841 ns** |     **1,760.6597 ns** |      **5,163.7104 ns** |     **30,512.1460 ns** | **1.023** |    **0.22** |   **0.4883** |    **2872 B** |        **1.00** |
| &#39;V.E.L.O.C.I.T.Y.-WorkFlow: O(1) Memory Pointer Cast State Resumption&#39;   | 10        |          0.0003 ns |         0.0012 ns |          0.0034 ns |          0.0000 ns | 0.000 |    0.00 |        - |         - |        0.00 |
|                                                                          |           |                    |                   |                    |                    |       |         |          |           |             |
| **&#39;Traditional Temporal: Full Event History Deserialization &amp; Replay Loop&#39;** | **100**       |    **490,314.0464 ns** |    **51,523.6433 ns** |    **151,918.6359 ns** |    **482,422.8516 ns** | **1.110** |    **0.52** |   **6.3477** |   **28073 B** |        **1.00** |
| &#39;V.E.L.O.C.I.T.Y.-WorkFlow: O(1) Memory Pointer Cast State Resumption&#39;   | 100       |          0.6127 ns |         0.2623 ns |          0.7735 ns |          0.1411 ns | 0.000 |    0.00 |        - |         - |        0.00 |
|                                                                          |           |                    |                   |                    |                    |       |         |          |           |             |
| **&#39;Traditional Temporal: Full Event History Deserialization &amp; Replay Loop&#39;** | **1000**      |  **2,904,897.8800 ns** |   **149,013.5216 ns** |    **429,938.1125 ns** |  **2,907,216.4062 ns** | **1.021** |    **0.21** |  **66.4063** |  **280083 B** |        **1.00** |
| &#39;V.E.L.O.C.I.T.Y.-WorkFlow: O(1) Memory Pointer Cast State Resumption&#39;   | 1000      |          0.6857 ns |         0.1820 ns |          0.5368 ns |          0.5073 ns | 0.000 |    0.00 |        - |         - |        0.00 |
|                                                                          |           |                    |                   |                    |                    |       |         |          |           |             |
| **&#39;Traditional Temporal: Full Event History Deserialization &amp; Replay Loop&#39;** | **10000**     | **43,029,938.5455 ns** | **3,526,313.4523 ns** | **10,397,415.5307 ns** | **40,981,054.5455 ns** | **1.056** |    **0.35** | **636.3636** | **2800318 B** |        **1.00** |
| &#39;V.E.L.O.C.I.T.Y.-WorkFlow: O(1) Memory Pointer Cast State Resumption&#39;   | 10000     |          0.4211 ns |         0.1449 ns |          0.4271 ns |          0.2869 ns | 0.000 |    0.00 |        - |         - |        0.00 |

# avro_poc
POC of Apache Avro Rust optimization

## Benchmark Results

### int

|        | `apache_avro`            | `avro_poc`                       |
|:-------|:-------------------------|:-------------------------------- |
|        | `65.29 ns` (✅ **1.00x**) | `31.63 ns` (🚀 **2.06x faster**)  |

### array

|        | `apache_avro`           | `avro_poc`                        |
|:-------|:------------------------|:--------------------------------- |
|        | `3.26 us` (✅ **1.00x**) | `821.55 ns` (🚀 **3.97x faster**)  |

### record

|        | `apache_avro`             | `avro_poc`                       |
|:-------|:--------------------------|:-------------------------------- |
|        | `721.90 ns` (✅ **1.00x**) | `78.84 ns` (🚀 **9.16x faster**)  |

### recursive

|        | `apache_avro`           | `avro_poc`                        |
|:-------|:------------------------|:--------------------------------- |
|        | `2.38 us` (✅ **1.00x**) | `373.80 ns` (🚀 **6.37x faster**)  |

### complex

|        | `apache_avro`           | `avro_poc`                         |
|:-------|:------------------------|:---------------------------------- |
|        | `3.29 us` (✅ **1.00x**) | `310.16 ns` (🚀 **10.61x faster**)  |

---
Made with [criterion-table](https://github.com/nu11ptr/criterion-table)


Storage Size Difference Between Nullable and Non-Nullable Columns in Parquet (for 1000 Rows)
Parquet uses definition levels to track NULL values, which introduce extra metadata storage for nullable columns. Let's break down the storage size for both columns.

🚀 Storage Calculation for Each Column
1️⃣ Non-Nullable Column (value - required int32)
Since it's always present, Parquet stores only the raw values.
Storage Size (in bytes):
int32 (4 bytes per row)
1000 rows × 4 bytes = 4000 bytes (4 KB)
✔ No extra storage overhead.

2️⃣ Nullable Column (id - optional int32)
Nullable columns require an extra "definition level" to track missing values.
The definition level uses 1-2 bits per row (depends on encoding).
Total storage includes:
Definition levels (~1.5 bits per row on average, stored as RLE/bit-packed)
Actual int32 values (only for non-null entries)
📌 Scenario 1: If All 1000 Rows Have Non-Null id Values
Definition levels: ~1.5 bits × 1000 rows = ~187.5 bytes
Actual int32 storage: 4 bytes × 1000 = 4000 bytes
Total Storage for id: 4188 bytes (~4.1 KB)
🚀 Overhead: ~188 bytes (~4.7% more than non-nullable column).

📌 Scenario 2: If 50% of id Values Are NULL
Definition levels: ~1.5 bits × 1000 rows = ~187.5 bytes
Actual int32 storage: 4 bytes × 500 non-null values = 2000 bytes
Total Storage for id: 2188 bytes (~2.1 KB)
🚀 Overhead remains (~188 bytes), but actual data stored is less.

📌 Scenario 3: If 90% of id Values Are NULL
Definition levels: ~1.5 bits × 1000 rows = ~187.5 bytes
Actual int32 storage: 4 bytes × 100 non-null values = 400 bytes
Total Storage for id: 588 bytes (~0.6 KB)
🚀 Significant storage savings due to Parquet’s efficient NULL handling.

🚀 Summary: Nullable vs. Non-Nullable Column Storage Size
Case	Non-Nullable (value)	Nullable (id)	Overhead
100% Non-Null (id)	4000 bytes	4188 bytes	~188 bytes (4.7%)
50% NULL (id)	4000 bytes	2188 bytes	~188 bytes (NULLs reduce data size)
90% NULL (id)	4000 bytes	588 bytes	Huge savings due to fewer stored values
🚀 Key Takeaways
✔ Nullable columns have a small metadata overhead (~188 bytes per 1000 rows).
✔ If most values are NULL, storage savings are significant.
✔ Definition levels (~1.5 bits per row) are well-optimized but still add overhead.
✔ Use required where possible to avoid metadata overhead & improve query efficiency.


### 1. **Non-Nullable Column (required int32 value)**

*   **Storage**:
    
    *   A non-nullable column does not require definition levels because all values are guaranteed to be non-null.
        
    *   Each int32 value occupies **4 bytes**.
        
*   **Total Storage for 1000 Rows**:
    
    *   1000 rows×4 bytes=4000 bytes1000 rows×4 bytes=4000 bytes.
        

### 2. **Nullable Column (optional int32 id)**

*   **Storage**:
    
    *   A nullable column requires **definition levels** to indicate whether a value is null or not.
        
    *   Each int32 value occupies **4 bytes** (if not null).
        
    *   Definition levels are stored using **Run-Length Encoding (RLE)** or **Bit Packing**, which typically adds **1 bit per row** to indicate null/non-null status.
        
*   **Assumptions**:
    
    *   Let's assume **50% of the values are null** (500 nulls and 500 non-nulls).
        
    *   Each non-null value occupies 4 bytes.
        
    *   Definition levels add 1 bit per row.
        
*   **Total Storage for 1000 Rows**:
    
    *   **Data Storage**:
        
        *   500 non-null values×4 bytes=2000 bytes500 non-null values×4 bytes=2000 bytes.
            
    *   **Definition Levels**:
        
        *   1000 rows×1 bit=1000 bits=125 bytes1000 rows×1 bit=1000 bits=125 bytes.
            
    *   **Total**:
        
        *   2000 bytes+125 bytes=2125 bytes2000 bytes+125 bytes=2125 bytes.
            

### 3. **Comparison**

*   **Non-Nullable Column (value)**:
    
    *   4000 bytes4000 bytes.
        
*   **Nullable Column (id)**:
    
    *   2125 bytes2125 bytes (with 50% null values).
        

### 4. **General Formula**

*   Total Storage=(Number of Non-Null Values×Data Size)+(Number of Rows8) bytesTotal Storage=(Number of Non-Null Values×Data Size)+(8Number of Rows​) bytes
    
    *   Where:
        
        *   Data Size=4 bytesData Size=4 bytes for int32.
            
        *   Number of Rows88Number of Rows​ converts bits to bytes for definition levels.
            
*   Total Storage=Number of Rows×Data SizeTotal Storage=Number of Rows×Data Size
    

### 5. **Example Calculation for Different Null Ratios**

**Null RatioNon-Null ValuesData Storage (bytes)Definition Levels (bytes)Total Storage (bytes)**0% (No Nulls)10001000×4=40001000×4=400010008=12581000​=1254000+125=41254000+125=412550%500500×4=2000500×4=200010008=12581000​=1252000+125=21252000+125=212590%100100×4=400100×4=40010008=12581000​=125400+125=525400+125=525

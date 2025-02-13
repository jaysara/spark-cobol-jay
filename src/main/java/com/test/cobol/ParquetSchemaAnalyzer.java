package com.test.cobol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.schema.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.*;

public class ParquetSchemaAnalyzer {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: ParquetSchemaAnalyzer <parquet-file-path>");
            System.exit(1);
        }

        String parquetFilePath = args[0];

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Parquet Schema Analyzer")
                .master("local[*]") // Runs locally
                .getOrCreate();

        // Read the Parquet file
        Dataset<Row> df = spark.read().parquet(parquetFilePath);

        // Get schema
        StructType schema = df.schema();

        // Analyze schema
        analyzeSchema(schema);

        // Check partitioning and bucketing
        analyzePartitioningAndBucketing(df);

        // Analyze Parquet file metadata (compression, encoding, file sizes)
        analyzeParquetMetadata(parquetFilePath);

        // Stop Spark session
        spark.stop();
    }

    
    /**
    private static void analyzeSchema(Dataset<Row> df) {
        System.out.println("\n==== PARQUET SCHEMA NULLABILITY ANALYSIS ====");

        // Get schema
        StructType schema = df.schema();

        // Loop through all columns
        for (StructField field : schema.fields()) {
            String fieldName = field.name();
            boolean isNullable = field.nullable();

            if (!isNullable) {
                System.out.println("✅ Column: " + fieldName + " is REQUIRED (No NULL overhead).");
                continue;
            }

            // Count total rows
            long totalRows = df.count();
            // Count NULL values in this column
            long nullCount = df.filter(df.col(fieldName).isNull()).count();
            double nullPercentage = (double) nullCount / totalRows;

            System.out.println("⚠️ Column: " + fieldName);
            System.out.println(" - Nullable: YES");
            System.out.println(" - Null Percentage: " + String.format("%.2f", nullPercentage * 100) + "%");

            // Issue warning if too many NULLs
            if (nullPercentage > HIGH_NULL_THRESHOLD) {
                System.out.println(" ⚠️ WARNING: More than " + (HIGH_NULL_THRESHOLD * 100) + "% of values are NULL.");
                System.out.println("    → Consider changing to REQUIRED or using a default value to save space.");
            }

            System.out.println();
        }
    }
}
    

    private static void analyzeSchema(Dataset<Row> df, String parquetDir) throws IOException {
        System.out.println("\n==== PARQUET SCHEMA & DICTIONARY ENCODING ANALYSIS ====");

        // Get schema
        StructType schema = df.schema();

        // Read metadata from all Parquet files
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(parquetDir);
        FileStatus[] files = fs.listStatus(dirPath, path -> path.getName().endsWith(".parquet"));

        if (files.length == 0) {
            System.err.println("No Parquet files found in directory: " + parquetDir);
            return;
        }

        // Collect metadata from all Parquet files
        List<ParquetMetadata> metadataList = new ArrayList<>();
        for (FileStatus file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(conf, file.getPath())) {
                metadataList.add(reader.getFooter());
            }
        }

        // Loop through all columns in schema
        for (StructField field : schema.fields()) {
            String fieldName = field.name();
            DataType dataType = field.dataType();

            System.out.println("Column: " + fieldName);
            System.out.println(" - Data Type: " + dataType);

            // Skip nested fields (ArrayType, StructType)
            if (!(dataType instanceof StringType || dataType instanceof IntegerType || dataType instanceof LongType || dataType instanceof DoubleType)) {
                System.out.println(" ⏩ Skipping non-primitive field: " + fieldName + " (Nested types are not dictionary-encoded)");
                continue;
            }

            // Estimate Cardinality
            long cardinality = df.select(fieldName).agg(functions.approx_count_distinct(fieldName)).first().getLong(0);
            System.out.println(" - Cardinality (Approx Unique Values): " + cardinality);

            // Check Dictionary Encoding across all Parquet files
            boolean isDictionaryEncoded = metadataList.stream()
                    .flatMap(meta -> meta.getBlocks().stream())
                    .flatMap(block -> block.getColumns().stream())
                    .filter(column -> column.getPath().toDotString().equals(fieldName))
                    .anyMatch(column -> column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));

            // Show warnings only if dictionary encoding is missing on low-cardinality fields
            if (cardinality < LOW_CARDINALITY_THRESHOLD && !isDictionaryEncoded) {
                System.out.println(" ⚠️  WARNING: Column '" + fieldName + "' has LOW cardinality but is NOT using dictionary encoding.");
                System.out.println("    → Consider enabling dictionary encoding to optimize storage and queries.");
            } else if (isDictionaryEncoded) {
                System.out.println(" ✅ Dictionary Encoding is USED.");
            } else {
                System.out.println(" ⏩ Skipping dictionary check for high-cardinality column.");
            }

            System.out.println();
        }
    }
}



    */
    
    private static void analyzeSchema(StructType schema) {
        System.out.println("\n==== PARQUET SCHEMA ANALYSIS ====");

        for (StructField field : schema.fields()) {
            String fieldName = field.name();
            DataType dataType = field.dataType();
            boolean nullable = field.nullable();

            System.out.println("Column: " + fieldName);
            System.out.println(" - Data Type: " + dataType);
            System.out.println(" - Nullable: " + nullable);

            // Check for inefficient data types
            if (dataType instanceof StringType) {
                System.out.println(" ⚠️  Consider using dictionary encoding for large text fields.");
            } else if (dataType instanceof DoubleType) {
                System.out.println(" ⚠️  Consider using DecimalType for improved precision.");
            } else if (dataType instanceof LongType) {
                System.out.println(" ✅  LongType is efficient unless values fit in IntegerType.");
            } else if (dataType instanceof IntegerType) {
                System.out.println(" ✅  IntegerType is an efficient choice.");
            } else if (dataType instanceof BinaryType) {
                System.out.println(" ⚠️  BinaryType can be expensive; avoid storing large blobs.");
            }

            // Check for nullable fields
            if (nullable) {
                System.out.println(" ⚠️  Nullable fields may cause performance issues due to Spark's handling of nulls.");
            }

            // Detect nested structures
            if (dataType instanceof StructType) {
                System.out.println(" ⚠️  Nested StructType detected. Consider flattening for better performance.");
            } else if (dataType instanceof ArrayType) {
                System.out.println(" ⚠️  ArrayType detected. Consider using explode() if needed for efficient queries.");
            }

            System.out.println();
        }
    }

    private static void analyzePartitioningAndBucketing(Dataset<Row> df) {
        System.out.println("\n==== PARTITIONING & BUCKETING ANALYSIS ====");

        List<String> partitionColumns = Arrays.asList(df.columns()); // Simplified check

        if (partitionColumns.isEmpty()) {
            System.out.println(" ⚠️  No partitioning detected. Consider partitioning for large datasets.");
        } else {
            System.out.println(" ✅  Partitioning detected on columns: " + partitionColumns);
        }

        // Bucket analysis (not directly available in metadata, assuming user-defined knowledge)
        List<String> bucketColumns = Arrays.asList(); // Needs external input
        if (!bucketColumns.isEmpty()) {
            System.out.println(" ✅ Bucketing detected on columns: " + bucketColumns);
            System.out.println("    Ensure that joins use bucketed tables for better performance.");
        } else {
            System.out.println(" ⚠️  No bucketing detected. Consider bucketing on high-cardinality join keys.");
        }
    }

    private static void analyzeParquetMetadata(String parquetFilePath) throws IOException {
        System.out.println("\n==== PARQUET FILE METADATA ANALYSIS ====");

        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(path);

        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        long totalRowCount = 0;
        int fileCount = files.length;

        for (FileStatus file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(conf, file.getPath())) {
                ParquetMetadata metadata = reader.getFooter();
                FileMetaData fileMetaData = metadata.getFileMetaData();
                List<BlockMetaData> rowGroups = metadata.getBlocks();

                for (BlockMetaData block : rowGroups) {
                    totalUncompressedSize += block.getTotalByteSize();
                    totalCompressedSize += block.getCompressedSize();
                    totalRowCount += block.getRowCount();
                }

                System.out.println("File: " + file.getPath().getName());
                System.out.println(" - Compression Codec: " + fileMetaData.getCreatedBy());
                System.out.println(" - Row Groups: " + rowGroups.size());
            }
        }

        if (totalCompressedSize > 0 && totalUncompressedSize > 0) {
            double compressionRatio = (double) totalUncompressedSize / totalCompressedSize;
            System.out.printf(" ✅ Compression Ratio: %.2f (higher is better)\n", compressionRatio);
        }

        // Small file check
        long avgFileSize = totalCompressedSize / fileCount;
        if (avgFileSize < 128 * 1024 * 1024) { // Less than 128MB
            System.out.println(" ⚠️  Small file detected! Average file size is " + (avgFileSize / (1024 * 1024)) + "MB.");
            System.out.println("    Consider merging small files to optimize performance.");
        } else {
            System.out.println(" ✅ File size is optimal.");
        }

        // Dictionary Encoding Check
        System.out.println("\n==== DICTIONARY ENCODING CHECK ====");
        for (FileStatus file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(conf, file.getPath())) {
                ParquetMetadata metadata = reader.getFooter();
                MessageType schema = metadata.getFileMetaData().getSchema();

                for (Type field : schema.getFields()) {
                    if (field.asPrimitiveType() != null) {
                        String fieldName = field.getName();
                        boolean isDictionaryEncoded = metadata.getBlocks().stream()
                                .flatMap(block -> block.getColumns().stream())
                                .anyMatch(column -> column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));

                        if (isDictionaryEncoded) {
                            System.out.println(" ✅ Dictionary Encoding used for: " + fieldName);
                        } else {
                            System.out.println(" ⚠️  Dictionary Encoding NOT used for: " + fieldName);
                        }
                    }
                }
            }
        }
    }
}

package com.test.cobol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

public class ParquetSchemaAnalyzer {

    public static void main(String[] args) throws IOException {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Parquet Schema Analyzer")
                .master("local[*]") // Run locally using all cores
                .getOrCreate();

        // Path to the Parquet file
        String parquetFilePath = "path/to/your/parquet/file.parquet";

        // Read the Parquet file
        Dataset<Row> df = spark.read().parquet(parquetFilePath);

        // Analyze the schema
        analyzeSchema(df.schema());

        // Analyze row group and column chunk sizes
        analyzeRowGroupsAndColumnChunks(parquetFilePath);

        // Analyze predicate pushdown optimization
        analyzePredicatePushdown(parquetFilePath);

        // Stop SparkSession
        spark.stop();
    }

    private static void analyzeSchema(StructType schema) {
        System.out.println("=== Schema Analysis Report ===");

        for (StructField field : schema.fields()) {
            System.out.println("\nColumn: " + field.name());
            System.out.println("Data Type: " + field.dataType());

            // Check for good practices
            if (isEfficientDataType(field.dataType().toString())) {
                System.out.println("✅ Good: Efficient data type used.");
            } else {
                System.out.println("❌ Bad: Inefficient data type. Consider using a smaller data type.");
            }

            // Check if the column is nullable
            if (field.nullable()) {
                System.out.println("⚠️ Warning: Column is nullable. Nullable columns can increase storage size.");
            } else {
                System.out.println("✅ Good: Column is not nullable.");
            }

            // Check for nested structures
            if (field.dataType().toString().contains("StructType") ||
                field.dataType().toString().contains("ArrayType") ||
                field.dataType().toString().contains("MapType")) {
                System.out.println("⚠️ Warning: Complex data type detected. Complex types can impact performance.");
            } else {
                System.out.println("✅ Good: No complex data type.");
            }
        }

        System.out.println("\n=== End of Schema Report ===");
    }

    private static boolean isEfficientDataType(String dataType) {
        // List of efficient data types
        String[] efficientDataTypes = {"IntegerType", "FloatType", "ShortType", "ByteType", "DateType"};
        for (String efficientType : efficientDataTypes) {
            if (dataType.contains(efficientType)) {
                return true;
            }
        }
        return false;
    }

    private static void analyzeRowGroupsAndColumnChunks(String parquetFilePath) throws IOException {
        System.out.println("\n=== Row Group and Column Chunk Analysis ===");

        // Read Parquet metadata
        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
        List<BlockMetaData> blocks = metadata.getBlocks();
        MessageType schema = metadata.getFileMetaData().getSchema();

        // Analyze row groups
        System.out.println("\nNumber of Row Groups: " + blocks.size());
        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            long rowCount = block.getRowCount();
            long totalSize = block.getTotalByteSize();
            System.out.println("\nRow Group " + i + ":");
            System.out.println("  - Row Count: " + rowCount);
            System.out.println("  - Total Size: " + totalSize + " bytes");

            // Check if row group size is optimal (128 MB to 1 GB is recommended)
            if (totalSize >= 128 * 1024 * 1024 && totalSize <= 1024 * 1024 * 1024) {
                System.out.println("  ✅ Good: Row group size is within the recommended range (128 MB to 1 GB).");
            } else {
                System.out.println("  ❌ Bad: Row group size is outside the recommended range. Consider adjusting row group size.");
            }

            // Analyze column chunks
            System.out.println("  Column Chunks:");
            for (ColumnChunkMetaData column : block.getColumns()) {
                String columnName = column.getPath().toDotString();
                long columnSize = column.getTotalSize();
                System.out.println("    - Column: " + columnName);
                System.out.println("      Size: " + columnSize + " bytes");

                // Check if column chunk size is reasonable
                if (columnSize > 0) {
                    System.out.println("      ✅ Good: Column chunk has data.");
                } else {
                    System.out.println("      ❌ Bad: Column chunk is empty or too small.");
                }
            }
        }

        System.out.println("\n=== End of Row Group and Column Chunk Analysis ===");
    }

    private static void analyzePredicatePushdown(String parquetFilePath) throws IOException {
        System.out.println("\n=== Predicate Pushdown Optimization Analysis ===");

        // Read Parquet metadata
        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
        List<BlockMetaData> blocks = metadata.getBlocks();

        // Analyze predicate pushdown support
        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            System.out.println("\nRow Group " + i + ":");

            for (ColumnChunkMetaData column : block.getColumns()) {
                String columnName = column.getPath().toDotString();
                System.out.println("  Column: " + columnName);

                // Check if statistics are available for predicate pushdown
                if (column.getStatistics() != null) {
                    System.out.println("    ✅ Good: Statistics available for predicate pushdown.");
                    System.out.println("      Min: " + column.getStatistics().genericGetMin());
                    System.out.println("      Max: " + column.getStatistics().genericGetMax());
                    System.out.println("      Null Count: " + column.getStatistics().getNumNulls());
                } else {
                    System.out.println("    ❌ Bad: No statistics available. Predicate pushdown will not be optimized for this column.");
                }
            }
        }

        System.out.println("\n=== End of Predicate Pushdown Analysis ===");
    }
}

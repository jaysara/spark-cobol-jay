package com.test.cobol;
import org.apache.parquet.schema.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

public class ParquetSchemaAnalyzer {

    public static void main(String[] args) {
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

        // Check if partitioning is used
        analyzePartitioning(df, parquetFilePath);

        // Stop Spark session
        spark.stop();
    }

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
                System.out.println(" ⚠️  Consider using DecimalType to improve precision.");
            } else if (dataType instanceof LongType) {
                System.out.println(" ✅  LongType is efficient unless smaller values fit in IntegerType.");
            } else if (dataType instanceof IntegerType) {
                System.out.println(" ✅  IntegerType is an efficient choice.");
            } else if (dataType instanceof BinaryType) {
                System.out.println(" ⚠️  BinaryType can be expensive; consider alternatives if storing large blobs.");
            }

            // Check for nullable fields
            if (nullable) {
                System.out.println(" ⚠️  Nullable fields may cause performance issues due to Spark's handling of nulls.");
            }

            System.out.println();
        }
    }

    private static void analyzePartitioning(Dataset<Row> df, String parquetFilePath) {
        System.out.println("\n==== PARTITIONING ANALYSIS ====");
        
        List<String> partitionColumns = df.inputFiles().length > 1 ? Arrays.asList(df.columns()) : List.of();

        if (partitionColumns.isEmpty()) {
            System.out.println(" ⚠️  No partitioning detected. Consider partitioning for large datasets.");
        } else {
            System.out.println(" ✅  Partitioning detected on columns: " + partitionColumns);
        }
    }
}

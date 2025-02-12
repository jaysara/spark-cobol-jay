package com.test.cobol;
  import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AnalyzeParquetSchema {

    public static void main(String[] args) {
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

        System.out.println("\n=== End of Report ===");
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
}

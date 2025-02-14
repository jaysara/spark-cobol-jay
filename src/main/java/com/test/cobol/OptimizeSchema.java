import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class OptimizeSchema {

    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Optimize Schema")
                .master("local[*]") // Run locally using all cores
                .getOrCreate();

        // Path to the input Parquet file or directory
        String inputPath = "path/to/your/input/parquet";
        // Path to the output Parquet file or directory
        String outputPath = "path/to/your/output/parquet";

        // Read the input Parquet file(s) into a DataFrame
        Dataset<Row> df = spark.read().parquet(inputPath);

        // Get the schema of the DataFrame
        StructType schema = df.schema();

        // Iterate through the schema to find array-type fields
        for (StructField field : schema.fields()) {
            if (field.dataType() instanceof ArrayType) {
                String arrayFieldName = field.name();
                System.out.println("Processing array field: " + arrayFieldName);

                // Count the number of rows with more than 1 element in the array
                long totalRows = df.count();
                long rowsWithMoreThanOneElement = df.filter(functions.size(functions.col(arrayFieldName)).gt(1)).count();
                double percentage = (double) rowsWithMoreThanOneElement / totalRows * 100;

                System.out.println("Percentage of rows with >1 element: " + percentage + "%");

                // Optimize the array field if less than 30% of rows have >1 element
                if (percentage < 30) {
                    System.out.println("Optimizing field: " + arrayFieldName);

                    // Flatten the first element of the array
                    df = df.withColumn(arrayFieldName + "_0", functions.col(arrayFieldName).getItem(0))
                           .withColumn(arrayFieldName + "_extra", functions.expr(
                               "CASE WHEN size(" + arrayFieldName + ") > 1 THEN slice(" + arrayFieldName + ", 2, size(" + arrayFieldName + ") - 1) ELSE array() END"
                           ));

                    // Drop the original array field
                    df = df.drop(arrayFieldName);
                }
            }
        }

        // Print the optimized schema
        System.out.println("Optimized Schema:");
        df.printSchema();

        // Write the optimized DataFrame back to Parquet
        df.write().parquet(outputPath);

        System.out.println("Optimized Parquet file written to: " + outputPath);

        // Stop SparkSession
        spark.stop();
    }
}

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

public class ParquetSchemaOptimizer {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ParquetSchemaOptimizer <inputParquetDir> <outputParquetDir>");
            System.exit(1);
        }

        String inputParquetDir = args[0];
        String outputParquetDir = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("ParquetSchemaOptimizer")
                .master("local[*]")  // Change this for cluster execution
                .getOrCreate();

        // Load the dataset
        Dataset<Row> df = spark.read().parquet(inputParquetDir);
        
        // Get schema and identify flattenable ArrayType fields
        StructType schema = df.schema();
        List<String> flattenableArrays = new ArrayList<>();

        for (StructField field : schema.fields()) {
            if (field.dataType() instanceof ArrayType) {
                String fieldName = field.name();
                
                // Count the distribution of array sizes
                Dataset<Row> counts = df.withColumn("array_size", functions.size(functions.col(fieldName)))
                        .groupBy("array_size").count().orderBy("array_size");

                // Count total rows
                long totalRows = df.count();
                Row[] zeroElementsRow = (Row[]) counts.filter("array_size = 0").head(1);
                long zeroElements = (zeroElementsRow.length > 0) ? zeroElementsRow[0].getLong(1) : 0;
                
                Row[] oneElementRow = (Row[]) counts.filter("array_size = 1").head(1);
                long oneElement = (oneElementRow.length > 0) ? oneElementRow[0].getLong(1) : 0;
                
                long totalRows = df.count();
                long multiElements = totalRows - (zeroElements + oneElement);

               
                double multiElementRatio = (double) multiElements / totalRows;
                
                // Flatten only if less than 30% of rows have >1 elements
                if (multiElementRatio < 0.3) {
                    flattenableArrays.add(fieldName);
                }
            }
        }

        System.out.println("📌 Fields to be flattened: " + flattenableArrays);

        // Apply transformations to flatten the dataset
        Dataset<Row> transformedDf = df;
        
        for (String fieldName : flattenableArrays) {
            // Extract the first element from the array
            transformedDf = transformedDf.withColumn(fieldName + "_flat", functions.expr(fieldName + "[0]"));
        
            // Extract remaining elements (if any) into a new column
            transformedDf = transformedDf.withColumn(fieldName + "_remaining", functions.expr("slice(" + fieldName + ", 2, size(" + fieldName + "))"));
        }

        // Drop original arrays that were flattened
        for (String fieldName : flattenableArrays) {
            transformedDf = transformedDf.drop(fieldName);
        }

        

        // Write optimized dataset
        transformedDf.write().mode(SaveMode.Overwrite).parquet(outputParquetDir);

        System.out.println("✅ Optimized dataset written to: " + outputParquetDir);
        spark.stop();
    }
}


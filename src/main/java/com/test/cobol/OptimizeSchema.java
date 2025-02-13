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
                           .withColumn(arrayFieldName + "_extra", functions.expr("slice(" + arrayFieldName + ", 2, size(" + arrayFieldName + ") - 1)"));

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

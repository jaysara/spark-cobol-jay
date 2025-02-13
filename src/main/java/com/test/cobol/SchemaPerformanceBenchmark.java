import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SchemaPerformanceBenchmark {

    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Schema Performance Benchmark")
                .master("local[*]") // Run locally using all cores
                .getOrCreate();

        // Paths to the original and optimized Parquet files
        String originalPath = "path/to/original/parquet";
        String optimizedPath = "path/to/optimized/parquet";

        // Read the original and optimized datasets
        Dataset<Row> originalDF = spark.read().parquet(originalPath);
        Dataset<Row> optimizedDF = spark.read().parquet(optimizedPath);

        // Measure query execution time on the original schema
        long startTime = System.currentTimeMillis();
        long originalCount = originalDF.filter("trds.acct-num = 123").count();
        long endTime = System.currentTimeMillis();
        System.out.println("Original Schema Query Execution Time: " + (endTime - startTime) + " ms");

        // Measure query execution time on the optimized schema
        startTime = System.currentTimeMillis();
        long optimizedCount = optimizedDF.filter("trds_0_acct_num = 123").count();
        endTime = System.currentTimeMillis();
        System.out.println("Optimized Schema Query Execution Time: " + (endTime - startTime) + " ms");

        // Print the query results (for verification)
        System.out.println("Original Schema Query Result: " + originalCount);
        System.out.println("Optimized Schema Query Result: " + optimizedCount);

        // Stop SparkSession
        spark.stop();
    }
}

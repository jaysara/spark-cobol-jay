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

    import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SchemaOptimizationBenchmark {
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("SchemaOptimizationBenchmark")
                .master("local[*]") // Use local mode for testing
                .getOrCreate();

        // Paths to the original and optimized Parquet files
        String originalParquetPath = "path/to/original/parquet/file";
        String optimizedParquetPath = "path/to/optimized/parquet/file";

        // Load the original and optimized DataFrames
        Dataset<Row> originalDf = spark.read().parquet(originalParquetPath);
        Dataset<Row> optimizedDf = spark.read().parquet(optimizedParquetPath);

        // Benchmark query on the original schema
        long startTimeOriginal = System.currentTimeMillis();
        Dataset<Row> originalQueryResult = originalDf.filter(
                functions.expr("exists(person, x -> x.fst_nme like 'J%')")
        );
        originalQueryResult.count(); // Trigger the query execution
        long endTimeOriginal = System.currentTimeMillis();
        long originalQueryTime = endTimeOriginal - startTimeOriginal;

        // Benchmark query on the optimized schema
        long startTimeOptimized = System.currentTimeMillis();
        Dataset<Row> optimizedQueryResult = optimizedDf.filter(
                functions.col("person_first_element.fst_nme").startsWith("J")
                        .or(functions.expr("exists(person_rest_elements, x -> x.fst_nme like 'J%')"))
        );
        optimizedQueryResult.count(); // Trigger the query execution
        long endTimeOptimized = System.currentTimeMillis();
        long optimizedQueryTime = endTimeOptimized - startTimeOptimized;

        // Print benchmark results
        System.out.println("Benchmark Results:");
        System.out.println("Original Schema Query Time: " + originalQueryTime + " ms");
        System.out.println("Optimized Schema Query Time: " + optimizedQueryTime + " ms");

        // Stop the Spark session
        spark.stop();
    }
}
}

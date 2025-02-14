import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class ParquetSchemaOptimizer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetSchemaOptimizer")
                .master("local[*]") // Adjust based on cluster
                .config("spark.sql.parquet.enable.dictionary", "true") // Enable dictionary encoding
                .getOrCreate();

        // Path to original Parquet dataset
        String inputPath = "input/parquet_dataset";
        String outputPath = "output/optimized_parquet";

        // Read original Parquet file
        Dataset<Row> df = spark.read().parquet(inputPath);

        // Analyze Schema and Optimize
        StructType originalSchema = df.schema();
        List<StructField> optimizedFields = new ArrayList<>();

        for (StructField field : originalSchema.fields()) {
            DataType fieldType = field.dataType();
            String fieldName = field.name();

            if (fieldType == DataTypes.LongType) {
                // Convert Long to Integer if within range
                long maxVal = df.agg(max(fieldName)).first().getLong(0);
                long minVal = df.agg(min(fieldName)).first().getLong(0);

                if (minVal >= Integer.MIN_VALUE && maxVal <= Integer.MAX_VALUE) {
                    optimizedFields.add(new StructField(fieldName, DataTypes.IntegerType, field.nullable(), field.metadata()));
                } else {
                    optimizedFields.add(field); // Keep as Long if outside Integer range
                }
            } else if (fieldType == DataTypes.StringType) {
                // Detect if String field has low cardinality (convert to categorical Integer)
                long distinctCount = df.select(fieldName).distinct().count();
                long rowCount = df.count();

                if (distinctCount < rowCount * 0.05) { // If less than 5% unique values
                    optimizedFields.add(new StructField(fieldName, DataTypes.IntegerType, field.nullable(), field.metadata()));
                } else {
                    optimizedFields.add(field);
                }
            } else {
                optimizedFields.add(field); // Keep other data types as-is
            }
        }

        // Create new schema
        StructType optimizedSchema = new StructType(optimizedFields.toArray(new StructField[0]));

        // Apply transformations based on schema optimizations
        Dataset<Row> optimizedDF = df;
        for (StructField field : optimizedSchema.fields()) {
            String fieldName = field.name();

            if (field.dataType() == DataTypes.IntegerType && df.schema().apply(fieldName).dataType() == DataTypes.StringType) {
                // Convert categorical String to Integer using a lookup table
                Dataset<Row> distinctValues = df.select(fieldName).distinct().withColumn("id", monotonically_increasing_id());
                optimizedDF = optimizedDF.join(distinctValues, fieldName).drop(fieldName).withColumnRenamed("id", fieldName);
            }
        }

        // Write optimized Parquet file
        optimizedDF.write().mode(SaveMode.Overwrite).parquet(outputPath);

        System.out.println("✅ Schema optimization complete! Optimized dataset written to: " + outputPath);
        
        // Stop Spark session
        spark.stop();
    }
}

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

public class ParquetLongToIntegerOptimizer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetLongToIntegerOptimizer")
                .master("local[*]")  // Adjust for cluster mode
                .config("spark.sql.parquet.enable.dictionary", "true")  // Enable dictionary encoding
                .getOrCreate();

        // Path to original Parquet dataset
        String inputPath = "input/parquet_dataset";
        String outputPath = "output/optimized_parquet";

        // Read original Parquet file
        Dataset<Row> df = spark.read().parquet(inputPath);
        StructType originalSchema = df.schema();

        // Threshold for low-cardinality optimization (e.g., < 0.1% of total rows)
        long rowCount = df.count();
        long cardinalityThreshold = (long) (rowCount * 0.001); // Adjust threshold as needed

        List<StructField> optimizedFields = new ArrayList<>();
        List<String> fieldsToConvert = new ArrayList<>();

        // Analyze schema & optimize LongType fields
        for (StructField field : originalSchema.fields()) {
            if (field.dataType() == DataTypes.LongType) {
                String fieldName = field.name();
                long distinctCount = df.select(fieldName).distinct().count();

                if (distinctCount < cardinalityThreshold) {
                    System.out.println("⚡ Optimizing field: " + fieldName + " (LongType → IntegerType)");
                    optimizedFields.add(new StructField(fieldName, DataTypes.IntegerType, field.nullable(), field.metadata()));
                    fieldsToConvert.add(fieldName);
                } else {
                    optimizedFields.add(field);
                }
            } else {
                optimizedFields.add(field);
            }
        }

        // Create new schema
        StructType optimizedSchema = new StructType(optimizedFields.toArray(new StructField[0]));

        // Convert Long fields to Integer in the dataset
        Dataset<Row> optimizedDF = df;
        for (String fieldName : fieldsToConvert) {
            optimizedDF = optimizedDF.withColumn(fieldName, col(fieldName).cast(DataTypes.IntegerType));
        }

        // Write optimized Parquet file
        optimizedDF.write().mode(SaveMode.Overwrite).parquet(outputPath);

        System.out.println("✅ Optimization complete! New dataset written to: " + outputPath);

        spark.stop();
    }
}


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;

public class DatasetComparator {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Dataset Comparator")
                .master("local[*]")
                .getOrCreate();

        // Example datasets
        Dataset<Row> dataset1 = spark.read().json("dataset1.json");
        Dataset<Row> dataset2 = spark.read().json("dataset2.json");

        // Perform a full outer join on the key field
        Dataset<Row> joined = dataset1.alias("ds1")
                .join(dataset2.alias("ds2"),
                        dataset1.col("KEY_FIELD1").equalTo(dataset2.col("KEY_FIELD1")),
                        "full_outer");

        // Compare fields
        Column comparisonResult = functions.struct(
                functions.when(dataset1.col("FLD3_NUM").equalTo(dataset2.col("FLD3_NUM")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD3_NUM_DIFF"),
                functions.when(dataset1.col("FLD1_NUM").equalTo(dataset2.col("FLD1_NUM")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD1_NUM_DIFF"),
                functions.when(dataset1.col("FLD2_NUM").equalTo(dataset2.col("FLD2_NUM")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD2_NUM_DIFF"),
                functions.when(dataset1.col("FLD1_VEC").equalTo(dataset2.col("FLD1_VEC")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD1_VEC_DIFF"),
                functions.when(dataset1.col("FLD2_VEC").equalTo(dataset2.col("FLD2_VEC")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD2_VEC_DIFF"),
                functions.when(dataset1.col("FLD3_GROUP").equalTo(dataset2.col("FLD3_GROUP")), "MATCH")
                        .otherwise("DIFFERENT").as("FLD3_GROUP_DIFF")
        );

        Dataset<Row> differences = joined.select(
                dataset1.col("KEY_FIELD1"),
                comparisonResult.as("FieldLevelComparison")
        );

        // Show the differences
        differences.show(false);
    }
}

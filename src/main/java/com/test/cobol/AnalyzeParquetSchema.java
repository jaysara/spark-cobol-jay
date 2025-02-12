import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ParquetSchemaAnalyzer {

    // Threshold for low cardinality (adjust as needed)
    private static final long LOW_CARDINALITY_THRESHOLD = 10000;

    // File path for the report
    private static final String REPORT_FILE_PATH = "parquet_analysis_report.txt";

    public static void main(String[] args) throws IOException {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Parquet Schema Analyzer")
                .master("local[*]") // Run locally using all cores
                .getOrCreate();

        // Path to the directory containing Parquet files
        String parquetDirPath = "path/to/your/parquetdir";

        // Read all Parquet files in the directory
        Dataset<Row> df = spark.read().parquet(parquetDirPath);

        // Open a file writer for the report
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(REPORT_FILE_PATH))) {
            // Analyze the schema
            analyzeSchema(df.schema(), writer);

            // Analyze row group and column chunk sizes for all files in the directory
            analyzeRowGroupsAndColumnChunks(parquetDirPath, df, writer);

            // Analyze predicate pushdown optimization for all files in the directory
            analyzePredicatePushdown(parquetDirPath, writer);
        }

        // Stop SparkSession
        spark.stop();
    }

    private static void analyzeSchema(StructType schema, BufferedWriter writer) throws IOException {
        writer.write("=== Schema Analysis Report ===\n");

        for (StructField field : schema.fields()) {
            writer.write("\nColumn: " + field.name() + "\n");
            writer.write("Data Type: " + field.dataType() + "\n");

            // Check for good practices
            if (isEfficientDataType(field.dataType().toString())) {
                writer.write("✅ Good: Efficient data type used.\n");
            } else {
                writer.write("❌ Bad: Inefficient data type. Consider using a smaller data type.\n");
            }

            // Check if the column is nullable
            if (field.nullable()) {
                writer.write("⚠️ Warning: Column is nullable. Nullable columns can increase storage size.\n");
            } else {
                writer.write("✅ Good: Column is not nullable.\n");
            }

            // Check for nested structures
            if (field.dataType().toString().contains("StructType") ||
                field.dataType().toString().contains("ArrayType") ||
                field.dataType().toString().contains("MapType")) {
                writer.write("⚠️ Warning: Complex data type detected. Complex types can impact performance.\n");
            } else {
                writer.write("✅ Good: No complex data type.\n");
            }
        }

        writer.write("\n=== End of Schema Report ===\n");
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

    private static void analyzeRowGroupsAndColumnChunks(String parquetDirPath, Dataset<Row> df, BufferedWriter writer) throws IOException {
        writer.write("\n=== Row Group and Column Chunk Analysis ===\n");

        // Get all Parquet files in the directory
        Configuration conf = new Configuration();
        Path dirPath = new Path(parquetDirPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().endsWith(".parquet"));

        // Iterate through each Parquet file
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            writer.write("\nAnalyzing file: " + filePath + "\n");

            try (ParquetFileReader reader = ParquetFileReader.open(conf, filePath)) {
                ParquetMetadata metadata = reader.getFooter();
                List<BlockMetaData> blocks = metadata.getBlocks();
                MessageType schema = metadata.getFileMetaData().getSchema();

                // Analyze row groups
                writer.write("\nNumber of Row Groups: " + blocks.size() + "\n");
                for (int i = 0; i < blocks.size(); i++) {
                    BlockMetaData block = blocks.get(i);
                    long rowCount = block.getRowCount();
                    long totalSize = block.getTotalByteSize();
                    writer.write("\nRow Group " + i + ":\n");
                    writer.write("  - Row Count: " + rowCount + "\n");
                    writer.write("  - Total Size: " + totalSize + " bytes\n");

                    // Check if row group size is optimal (128 MB to 1 GB is recommended)
                    if (totalSize >= 128 * 1024 * 1024 && totalSize <= 1024 * 1024 * 1024) {
                        writer.write("  ✅ Good: Row group size is within the recommended range (128 MB to 1 GB).\n");
                    } else {
                        writer.write("  ❌ Bad: Row group size is outside the recommended range. Consider adjusting row group size.\n");
                    }

                    // Analyze column chunks
                    writer.write("  Column Chunks:\n");
                    for (ColumnChunkMetaData column : block.getColumns()) {
                        String columnName = column.getPath().toDotString();
                        long columnSize = column.getTotalSize();
                        writer.write("    - Column: " + columnName + "\n");
                        writer.write("      Size: " + columnSize + " bytes\n");

                        // Check if column chunk size is reasonable
                        if (columnSize > 0) {
                            writer.write("      ✅ Good: Column chunk has data.\n");
                        } else {
                            writer.write("      ❌ Bad: Column chunk is empty or too small.\n");
                        }

                        // Check dictionary encoding
                        if (!column.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
                            // Try to find the column in the DataFrame schema
                            StructField field = findColumnInSchema(df.schema(), columnName);
                            if (field != null) {
                                DataType dataType = field.dataType();

                                // Calculate cardinality based on data type
                                long cardinality;
                                if (dataType instanceof ArrayType) {
                                    // Flatten the array column and calculate cardinality
                                    cardinality = df.select(functions.explode(functions.col(columnName)).as("flattened"))
                                                   .distinct()
                                                   .count();
                                } else if (dataType instanceof MapType) {
                                    // Flatten the map column (keys and values) and calculate cardinality
                                    cardinality = df.select(functions.explode(functions.map_keys(functions.col(columnName))).as("flattened"))
                                                   .distinct()
                                                   .count();
                                } else {
                                    // Calculate cardinality for non-complex types
                                    cardinality = df.select(columnName).distinct().count();
                                }

                                // Display warning if cardinality is low
                                if (cardinality < LOW_CARDINALITY_THRESHOLD) {
                                    writer.write("      ⚠️ Warning: Dictionary encoding is not used, but cardinality is low (" + cardinality + "). Consider enabling dictionary encoding for better performance.\n");
                                }
                            } else {
                                writer.write("      ⚠️ Warning: Column '" + columnName + "' not found in DataFrame schema. Skipping cardinality calculation.\n");
                            }
                        }
                    }
                }
            }
        }

        writer.write("\n=== End of Row Group and Column Chunk Analysis ===\n");
    }

    private static void analyzePredicatePushdown(String parquetDirPath, BufferedWriter writer) throws IOException {
        writer.write("\n=== Predicate Pushdown Optimization Analysis ===\n");

        // Get all Parquet files in the directory
        Configuration conf = new Configuration();
        Path dirPath = new Path(parquetDirPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().endsWith(".parquet"));

        // Iterate through each Parquet file
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            writer.write("\nAnalyzing file: " + filePath + "\n");

            try (ParquetFileReader reader = ParquetFileReader.open(conf, filePath)) {
                ParquetMetadata metadata = reader.getFooter();
                List<BlockMetaData> blocks = metadata.getBlocks();

                // Analyze predicate pushdown support
                for (int i = 0; i < blocks.size(); i++) {
                    BlockMetaData block = blocks.get(i);
                    writer.write("\nRow Group " + i + ":\n");

                    for (ColumnChunkMetaData column : block.getColumns()) {
                        String columnName = column.getPath().toDotString();
                        writer.write("  Column: " + columnName + "\n");

                        // Check if statistics are available for predicate pushdown
                        if (column.getStatistics() != null) {
                            writer.write("    ✅ Good: Statistics available for predicate pushdown.\n");
                            writer.write("      Min: " + column.getStatistics().genericGetMin() + "\n");
                            writer.write("      Max: " + column.getStatistics().genericGetMax() + "\n");
                            writer.write("      Null Count: " + column.getStatistics().getNumNulls() + "\n");
                        } else {
                            writer.write("    ❌ Bad: No statistics available. Predicate pushdown will not be optimized for this column.\n");
                        }
                    }
                }
            }
        }

        writer.write("\n=== End of Predicate Pushdown Analysis ===\n");
    }

    // Helper method to find a column in the schema (handles nested columns)
    import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ParquetSchemaAnalyzer {

    // Threshold for low cardinality (adjust as needed)
    private static final long LOW_CARDINALITY_THRESHOLD = 10000;

    // File path for the report
    private static final String REPORT_FILE_PATH = "parquet_analysis_report.txt";

    public static void main(String[] args) throws IOException {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Parquet Schema Analyzer")
                .master("local[*]") // Run locally using all cores
                .getOrCreate();

        // Path to the directory containing Parquet files
        String parquetDirPath = "path/to/your/parquetdir";

        // Read all Parquet files in the directory
        Dataset<Row> df = spark.read().parquet(parquetDirPath);

        // Open a file writer for the report
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(REPORT_FILE_PATH))) {
            // Analyze the schema
            analyzeSchema(df.schema(), writer);

            // Analyze row group and column chunk sizes for all files in the directory
            analyzeRowGroupsAndColumnChunks(parquetDirPath, df, writer);

            // Analyze predicate pushdown optimization for all files in the directory
            analyzePredicatePushdown(parquetDirPath, writer);
        }

        // Stop SparkSession
        spark.stop();
    }

    private static void analyzeSchema(StructType schema, BufferedWriter writer) throws IOException {
        writer.write("=== Schema Analysis Report ===\n");

        for (StructField field : schema.fields()) {
            writer.write("\nColumn: " + field.name() + "\n");
            writer.write("Data Type: " + field.dataType() + "\n");

            // Check for good practices
            if (isEfficientDataType(field.dataType().toString())) {
                writer.write("✅ Good: Efficient data type used.\n");
            } else {
                writer.write("❌ Bad: Inefficient data type. Consider using a smaller data type.\n");
            }

            // Check if the column is nullable
            if (field.nullable()) {
                writer.write("⚠️ Warning: Column is nullable. Nullable columns can increase storage size.\n");
            } else {
                writer.write("✅ Good: Column is not nullable.\n");
            }

            // Check for nested structures
            if (field.dataType().toString().contains("StructType") ||
                field.dataType().toString().contains("ArrayType") ||
                field.dataType().toString().contains("MapType")) {
                writer.write("⚠️ Warning: Complex data type detected. Complex types can impact performance.\n");
            } else {
                writer.write("✅ Good: No complex data type.\n");
            }
        }

        writer.write("\n=== End of Schema Report ===\n");
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

    private static void analyzeRowGroupsAndColumnChunks(String parquetDirPath, Dataset<Row> df, BufferedWriter writer) throws IOException {
        writer.write("\n=== Row Group and Column Chunk Analysis ===\n");

        // Get all Parquet files in the directory
        Configuration conf = new Configuration();
        Path dirPath = new Path(parquetDirPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().endsWith(".parquet"));

        // Iterate through each Parquet file
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            writer.write("\nAnalyzing file: " + filePath + "\n");

            try (ParquetFileReader reader = ParquetFileReader.open(conf, filePath)) {
                ParquetMetadata metadata = reader.getFooter();
                List<BlockMetaData> blocks = metadata.getBlocks();
                MessageType schema = metadata.getFileMetaData().getSchema();

                // Analyze row groups
                writer.write("\nNumber of Row Groups: " + blocks.size() + "\n");
                for (int i = 0; i < blocks.size(); i++) {
                    BlockMetaData block = blocks.get(i);
                    long rowCount = block.getRowCount();
                    long totalSize = block.getTotalByteSize();
                    writer.write("\nRow Group " + i + ":\n");
                    writer.write("  - Row Count: " + rowCount + "\n");
                    writer.write("  - Total Size: " + totalSize + " bytes\n");

                    // Check if row group size is optimal (128 MB to 1 GB is recommended)
                    if (totalSize >= 128 * 1024 * 1024 && totalSize <= 1024 * 1024 * 1024) {
                        writer.write("  ✅ Good: Row group size is within the recommended range (128 MB to 1 GB).\n");
                    } else {
                        writer.write("  ❌ Bad: Row group size is outside the recommended range. Consider adjusting row group size.\n");
                    }

                    // Analyze column chunks
                    writer.write("  Column Chunks:\n");
                    for (ColumnChunkMetaData column : block.getColumns()) {
                        String columnName = column.getPath().toDotString();
                        long columnSize = column.getTotalSize();
                        writer.write("    - Column: " + columnName + "\n");
                        writer.write("      Size: " + columnSize + " bytes\n");

                        // Check if column chunk size is reasonable
                        if (columnSize > 0) {
                            writer.write("      ✅ Good: Column chunk has data.\n");
                        } else {
                            writer.write("      ❌ Bad: Column chunk is empty or too small.\n");
                        }

                        // Check dictionary encoding
                        if (!column.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
                            // Try to find the column in the DataFrame schema
                            StructField field = findColumnInSchema(df.schema(), columnName);
                            if (field != null) {
                                DataType dataType = field.dataType();

                                // Calculate cardinality based on data type
                                long cardinality;
                                if (dataType instanceof ArrayType) {
                                    // Flatten the array column and calculate cardinality
                                    cardinality = df.select(functions.explode(functions.col(columnName)).as("flattened"))
                                                   .distinct()
                                                   .count();
                                } else if (dataType instanceof MapType) {
                                    // Flatten the map column (keys and values) and calculate cardinality
                                    cardinality = df.select(functions.explode(functions.map_keys(functions.col(columnName))).as("flattened"))
                                                   .distinct()
                                                   .count();
                                } else {
                                    // Calculate cardinality for non-complex types
                                    cardinality = df.select(columnName).distinct().count();
                                }

                                // Display warning if cardinality is low
                                if (cardinality < LOW_CARDINALITY_THRESHOLD) {
                                    writer.write("      ⚠️ Warning: Dictionary encoding is not used, but cardinality is low (" + cardinality + "). Consider enabling dictionary encoding for better performance.\n");
                                }
                            } else {
                                writer.write("      ⚠️ Warning: Column '" + columnName + "' not found in DataFrame schema. Skipping cardinality calculation.\n");
                            }
                        }
                    }
                }
            }
        }

        writer.write("\n=== End of Row Group and Column Chunk Analysis ===\n");
    }

    private static void analyzePredicatePushdown(String parquetDirPath, BufferedWriter writer) throws IOException {
        writer.write("\n=== Predicate Pushdown Optimization Analysis ===\n");

        // Get all Parquet files in the directory
        Configuration conf = new Configuration();
        Path dirPath = new Path(parquetDirPath);
        FileSystem fs = dirPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().endsWith(".parquet"));

        // Iterate through each Parquet file
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            writer.write("\nAnalyzing file: " + filePath + "\n");

            try (ParquetFileReader reader = ParquetFileReader.open(conf, filePath)) {
                ParquetMetadata metadata = reader.getFooter();
                List<BlockMetaData> blocks = metadata.getBlocks();

                // Analyze predicate pushdown support
                for (int i = 0; i < blocks.size(); i++) {
                    BlockMetaData block = blocks.get(i);
                    writer.write("\nRow Group " + i + ":\n");

                    for (ColumnChunkMetaData column : block.getColumns()) {
                        String columnName = column.getPath().toDotString();
                        writer.write("  Column: " + columnName + "\n");

                        // Check if statistics are available for predicate pushdown
                        if (column.getStatistics() != null) {
                            writer.write("    ✅ Good: Statistics available for predicate pushdown.\n");
                            writer.write("      Min: " + column.getStatistics().genericGetMin() + "\n");
                            writer.write("      Max: " + column.getStatistics().genericGetMax() + "\n");
                            writer.write("      Null Count: " + column.getStatistics().getNumNulls() + "\n");
                        } else {
                            writer.write("    ❌ Bad: No statistics available. Predicate pushdown will not be optimized for this column.\n");
                        }
                    }
                }
            }
        }

        writer.write("\n=== End of Predicate Pushdown Analysis ===\n");
    }

    // Helper method to find a column in the schema (handles nested columns and arrays/maps)
    private static StructField findColumnInSchema(StructType schema, String columnName) {
        // Split the column name by dots to handle nested columns
        String[] parts = columnName.split("\\.");
        StructField field = null;
        StructType currentSchema = schema;

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            field = null; // Reset field for each part

            // Search for the field in the current schema
            for (StructField f : currentSchema.fields()) {
                if (f.name().equals(part)) {
                    field = f;
                    break;
                }
            }

            if (field == null) {
                return null; // Column not found
            }

            // Handle ArrayType
            if (field.dataType() instanceof ArrayType) {
                DataType elementType = ((ArrayType) field.dataType()).elementType();
                if (elementType instanceof StructType) {
                    currentSchema = (StructType) elementType; // Traverse the element type's schema
                } else {
                    // If the element type is not a StructType, we cannot traverse further
                    return (i == parts.length - 1) ? field : null;
                }
            }
            // Handle MapType
            else if (field.dataType() instanceof MapType) {
                DataType valueType = ((MapType) field.dataType()).valueType();
                if (valueType instanceof StructType) {
                    currentSchema = (StructType) valueType; // Traverse the value type's schema
                } else {
                    // If the value type is not a StructType, we cannot traverse further
                    return (i == parts.length - 1) ? field : null;
                }
            }
            // Handle StructType
            else if (field.dataType() instanceof StructType) {
                currentSchema = (StructType) field.dataType(); // Traverse nested schema
            }
            // Handle primitive types
            else {
                // If the field is not a StructType, ArrayType, or MapType, we cannot traverse further
                return (i == parts.length - 1) ? field : null;
            }
        }

        return field;
    }
}
}

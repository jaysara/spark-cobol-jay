package com.test.cobol;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class FixedWidthApp
{
    public static void main( String[] args )
    {
        FixedWidthApp app = new FixedWidthApp();
        System.out.println("Current JVM version - " + System.getProperty("java.version"));
        app.start();
    }

    private void start()
    {

        SparkSession spark = SparkSession.builder()
                .appName("Cobol FixedWidth")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        String copybook =
                "        01  RECORD.\n" +
                        "           05  RECORD_LENGTH_FIELD       PIC 9(4).\n" +
                        "           05  SEGMENT                   PIC X(123).\n" ;

        Dataset<Row> df1 =  spark.read()
                .format("za.co.absa.cobrix.spark.cobol.source")
                .option("copybook_contents", copybook)
                .option("encoding", "ascii")
                .option("record_format", "V") // Variable length records
                .option("record_length_field", "RECORD_LENGTH_FIELD")
                .option("schema_retention_policy", "collapse_root")
                .option("file_start_offset", "100") // skip file header
                .option("file_end_offset", "15") // skip file footer
                .load("data/variable-length-file.txt");

        df1.printSchema();

        df1.show();
    }
}

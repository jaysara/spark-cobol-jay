package com.test.cobol;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class EbcDicPackedApp
{
    public static void main( String[] args )
    {
        EbcDicPackedApp app = new EbcDicPackedApp();
        System.out.println("Current JVM version - " + System.getProperty("java.version"));
        app.start();
    }

    private void start()
    {

        SparkSession spark = SparkSession.builder()
                .appName("Cobol EbcdicPacked")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")

                .getOrCreate();

        String copybook =
                "        01  RECORD.\n" +
//                        "           05  RECORD_LENGTH_FIELD       PIC 9(4).\n" +
                        "           05  SEGMENT                   PIC X(1000).\n" ;

        Dataset<Row> df1 =  spark.read()
                .format("za.co.absa.cobrix.spark.cobol.source")
                .option("copybook_contents", copybook)
                .option("encoding", "ebcdic")
                .option("record_format", "V") // Variable length records
                .option("is_rdw_big_endian",true)
                .option("rdw_adjustment",-4)
//                .option("record_length_field", "RECORD_LENGTH_FIELD")
                .option("schema_retention_policy", "collapse_root")
//                .option("file_start_offset", "100") // skip file header
//                .option("file_end_offset", "15") // skip file footer
                .load("data/rdw-sample-ebcdic-packed.dat");

        df1.printSchema();
        System.out.println("df2 count "+df1.count());
        df1.show(false);
//        df1.foreach();
    }
}

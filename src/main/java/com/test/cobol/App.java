package com.test.cobol;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        App app = new App();
        app.start();
    }

    private void start()
    {

        SparkSession spark = SparkSession.builder()
                .appName("Cobol Dataset")
                .master("local")
                .getOrCreate();

        String copybook =
                "        01  RECORD.\n" +
                        "           05  ID                        PIC S9(4)  COMP.\n" +
                        "           05  COMPANY.\n" +
                        "               10  SHORT-NAME            PIC X(10).\n" +
                        "               10  COMPANY-ID-NUM        PIC 9(5) COMP-3.\n" +
                        "               10  COMPANY-ID-STR\n" +
                        "			         REDEFINES  COMPANY-ID-NUM PIC X(3).\n" +
                        "           05  METADATA.\n" +
                        "               10  CLIENTID              PIC X(15).\n" +
                        "               10  REGISTRATION-NUM      PIC X(10).\n" +
                        "               10  NUMBER-OF-ACCTS       PIC 9(03) COMP-3.\n" +
                        "               10  ACCOUNT.\n" +
                        "                   12  ACCOUNT-DETAIL    OCCURS 80\n" +
                        "                                         DEPENDING ON NUMBER-OF-ACCTS.\n" +
                        "                      15  ACCOUNT-NUMBER     PIC X(24).\n" +
                        "                      15  ACCOUNT-TYPE-N     PIC 9(5) COMP-3.\n" +
                        "                      15  ACCOUNT-TYPE-X     REDEFINES\n" +
                        "                           ACCOUNT-TYPE-N  PIC X(3).\n";
        Dataset<Row> df1 =  spark.read()
                .format("za.co.absa.cobrix.spark.cobol.source")
                .option("copybook_contents", copybook)
                .option("schema_retention_policy", "collapse_root")
                .load("/Users/jay/Downloads/file1.bin");

        df1.printSchema();

        df1.show();
    }
}

package com.test.cobol;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
                .option("generate_record_bytes", "true")
//                .option("record_length_field", "RECORD_LENGTH_FIELD")
                .option("schema_retention_policy", "collapse_root")
                .option("debug", "true")
                .option("debug", "binary")
                .option("file_start_offset", "100") // skip file header
//                .option("file_end_offset", "15") // skip file footer
                .load("data/rdw-sample-ebcdic-packed.dat");

        df1.printSchema();
        System.out.println("df2 count "+df1.count());
        df1.show(false);
        df1.foreach(foreachByteRow);
    }

    static ForeachFunction foreachByteRow = new ForeachFunction<Row>() {
        @Override
        public void call(Row row) throws Exception {

           // row.get
            byte[]  rowByte = (byte[])row.get(0);
            System.out.println(PackedPerson.createPersonFromRawBytes(rowByte).toString());
//            System.out.println(" length of rowByte "+rowByte.length);
//            byte[] p1dateByte = Arrays.copyOfRange(rowByte,20,25);
//            int DropHO = 0xFF;
//            for(int i=0;i<p1dateByte.length;i++)
//            {
//                int aByte = p1dateByte[i] & DropHO;
//                System.out.println("Byte "+i+" is "+aByte);
//            }
//            long datefield = PackedDecimal.parse(p1dateByte);
//            byte[] fNameByte = Arrays.copyOfRange(rowByte,0,10);
//            String firstName = new String(fNameByte,"ibm500");
//            byte[] lNameByte = Arrays.copyOfRange(rowByte,10,20);
//            String lName = new String(lNameByte,"ibm500");
//
//            System.out.println("Fist name "+firstName+ " Last Name "+lName+" DOB "+datefield);
        }
    };
    static ForeachFunction foreach =new ForeachFunction<Row>() {
        @Override
        public void call(Row row) throws Exception {
            System.out.println("class name "+row.get(0).getClass().getName());
            String rowStr = row.getString(0);
            if (rowStr.contains("Matt"))
            {
               byte[] p1 = PackedPerson.getPersonInBytes(new PackedPerson("Matt","Smith","19880608","Male",5443));
                int personLength = 10+10+7+5+3;
                int rdw = personLength+4;
                System.out.println("Total string length "+rowStr.length());
                System.out.println("Raw String "+rowStr);
                System.out.println("Parsing with out converting to EBCDIC");
                System.out.println("Fist name "+rowStr.substring(0,10)+ " Last Name "+rowStr.substring(10,20));

                PackedDecimal packedDecimal = new PackedDecimal(19880608,5);
                 byte[] dateconver = packedDecimal.toByteArray();
                byte[] datepackedBytes = rowStr.substring(20,25).getBytes("ibm500");

                byte[] p1dateByte = Arrays.copyOfRange(p1,24,29);

                System.out.println("Date orgianal length is "+dateconver.length);
                System.out.println("Date spark length is "+datepackedBytes.length);
                System.out.println(" are two arrays equal ? "+Arrays.equals(dateconver,datepackedBytes));
                int DropHO = 0xFF;
                for(int i=0;i<dateconver.length;i++)
                {
                    int aByte = dateconver[i] & DropHO;
                    int bByte = datepackedBytes[i] & DropHO;
                    int cByte = p1dateByte[i] & DropHO;
                    System.out.println(" i = "+i+ " Original Byte "+aByte+" New Byte "+bByte+ " orgianl file byte "+cByte);
                }
//                System.out.println("Original date "+ PackedDecimal.parse(dateconver));
//                long datevale = PackedDecimal.parse(datepackedBytes);
//                System.out.println("Date value is "+datevale);
//                packedDecimal = new PackedDecimal(Integer.parseInt(person.getDob()),5);
//                byte[] dobpackedbytes =  packedDecimal.toByteArray();
//                byteBuffer.put(dobpackedbytes);

                byte[] strBytes = rowStr.getBytes("ibm500");
                System.out.println("Lenght of string "+ strBytes.length);
//                System.out.println("Fist Name : "+strBytes);
            }
        }
    };
}

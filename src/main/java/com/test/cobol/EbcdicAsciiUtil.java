package com.test.cobol;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class EbcdicAsciiUtil {

    public static void main(String[] args) throws Exception {

        File outputfile = new File("/Users/jay/Workspace/spark-cobol-jay/data/rdw-sample-ebcdic-packed.dat");
//        outputfile.get
        String content = new String(Files.readAllBytes(Paths.get("/Users/jay/Workspace/spark-cobol-jay/data/rdw-sample-ebcdic-packed.dat")));
//        System.out.println(content);
//
        byte[] ebcdicBytes = Files.readAllBytes(Paths.get("/Users/jay/Workspace/spark-cobol-jay/data/rdw-sample-ebcdic-packed.dat"));
        System.out.println("Ebcdic byte size "+ebcdicBytes.length);
        byte[] firstRecord = Arrays.copyOfRange(ebcdicBytes, 104, 129 );
        byte[] firstRecordDate = Arrays.copyOfRange(ebcdicBytes, 124, 129 );
        int DropHO = 0xFF;
        for(int i=0;i<firstRecordDate.length;i++)
        {
            int aByte = firstRecordDate[i] & DropHO;
            System.out.println("Byte "+i+" is "+aByte);
        }
//
        System.out.println("First record byte array length "+firstRecord.length);
        String parsesEbcdicFirstRecord = new String (firstRecord,"ibm500");
        System.out.println(parsesEbcdicFirstRecord);
        System.out.println("Length of first record : "+parsesEbcdicFirstRecord.length());

        byte[] firstRecordFromStringbytes = parsesEbcdicFirstRecord.substring(20).getBytes("ibm500");
        for(int i=0;i<firstRecordFromStringbytes.length;i++)
        {
            int aByte = firstRecordFromStringbytes[i] & DropHO;
            System.out.println("firstRecordFromStringbytes-Byte "+i+" is "+aByte);
        }
//        File outputfile = new File("/Users/jay/Workspace/spark-cobol-jay/data/bdw-rdw-sample-ebcdic.dat");
//        Files.write(outputfile.toPath(),ebcdicBytes);
//
//        byte[] fileEbcdicBytes = Files.readAllBytes(outputfile.toPath());
//        String parsesEbcdic = new String (fileEbcdicBytes,"ibm500");
//        System.out.println(parsesEbcdic);
//        System.out.println("Contentss are same ? "+ parsesEbcdic.compareTo(content));
//        createpackedDecimalEbcdicfile();
    }

    public  static void createpackedDecimalEbcdicfile() throws IOException {
        byte[] p1 = PackedPerson.getPersonInBytes(new PackedPerson("Matt","Smith","19880608","Male",5443));
        byte[] p2 = PackedPerson.getPersonInBytes(new PackedPerson("John","McKowski","19930512","Male",7766));
        byte[] p3 = PackedPerson.getPersonInBytes(new PackedPerson("Gina","Smith","19890608","Female",885));
        byte[] p4 = PackedPerson.getPersonInBytes(new PackedPerson("Joseph","McMillonH","19890608","Male",1234));
        byte[] header = String.format("%-96s","HEADER    NOT IMPORTANT").getBytes("ibm500");
        byte[] headersizebytes = ByteBuffer.allocate(4).putShort((short)100).array();

        byte[] headerBytes = new byte[100];
        ByteBuffer byteBuffer
                = ByteBuffer.wrap(headerBytes);
        byteBuffer.put(headersizebytes);
        byteBuffer.put(header);
        byte[] allheaderbytes = byteBuffer.array();
        System.out.println("Heder length = "+allheaderbytes.length);
        File outputfile = new File("/Users/jay/Workspace/spark-cobol-jay/data/rdw-sample-ebcdic-packed.dat");
        ByteBuffer file = ByteBuffer.wrap(new byte[allheaderbytes.length+p1.length+p2.length+p3.length+p4.length]);
        file.put(allheaderbytes);
        file.put(p1);
        file.put(p2);
        file.put(p3);
        file.put(p4);
        byte[] filewrite = file.array();
        System.out.println("TOtal record length "+filewrite.length);
        Files.write(outputfile.toPath(),filewrite);
//        PackedDecimal packedDecimal = new PackedDecimal(1254,3);
//        byte[] byteconver =  packedDecimal.toByteArray();
////        byte[] lbytes = new byte[] { 0x12, (byte)0x88, 0x04, 0x3c };
//        System.out.println("Lenght of bytes "+byteconver.length);
//
//        packedDecimal = new PackedDecimal(19880608,5);
//        byte[] dateconver = packedDecimal.toByteArray();
//
////        packedDecimal = new PackedDecimal(lbytes);
//        System.out.println(" converted amount is "+PackedDecimal.parse(byteconver));
//        System.out.println(" converted Date is "+PackedDecimal.parse(dateconver));
//        System.out.println("Lenght of lbytes "+dateconver.length);
//
//        String content = "HelloJay";
//        System.out.println("String length "+content.length());
//        System.out.println("Byte lengh length "+content.getBytes().length);
//
//        byte[] ebcdicBytes = content.getBytes("ibm500");
//        System.out.println("Ebcdic byte lenght "+ebcdicBytes.length);
//        int m = 23;
//        System.out.println("output :"+String.format("%04d",m));


    }

}

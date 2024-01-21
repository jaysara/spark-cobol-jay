package com.test.cobol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PackedPerson {

    private String fname;
    private String lname;
    private String dob;
    private String gender;
    private int amount;

    public static String createStringFromPerson(PackedPerson person)
    {
        StringBuilder stringBuilder = new StringBuilder(63);
        stringBuilder
                .append(String.format("%-10s",person.getFname()))
                .append(String.format("%-10s",person.getLname()))
                .append(String.format("%-8s",person.getDob()))//5 bytes
                .append(String.format("%-7s",person.getGender()))
                .append(String.format("%-8s",person.getAmount()));//3 bytes
        String answer = stringBuilder.toString();
        System.out.println("Length of string is "+answer.length());
        return answer;
    }
    public static PackedPerson createPersonFromRawBytes(byte[] personBytes) throws Exception {
        String ebcdicFormat = "ibm500";
        return new PackedPerson(new String(Arrays.copyOfRange(personBytes,0,10),ebcdicFormat),
        new String(Arrays.copyOfRange(personBytes,10,20),ebcdicFormat),
                Long.toString(PackedDecimal.parse(Arrays.copyOfRange(personBytes,20,25))),
                new String(Arrays.copyOfRange(personBytes,25,32),ebcdicFormat),
                Integer.parseInt(Long.toString(PackedDecimal.parse(Arrays.copyOfRange(personBytes,32,35)))))
                ;
    }
    public static byte[] getPersonInBytes(PackedPerson person) throws UnsupportedEncodingException {
//        ByteBuffer byteBuffer  = new ByteBuffer();
        //7  for genger , 5 packed for DOB AND 3  packed for amount // 4 for RDW
        int personLength = 10+10+7+5+3;
        int rdw = personLength+4;
        byte[] rdwbytes = ByteBuffer.allocate(4).putShort((short)rdw).array();
//        String rdwStr = String.format("%04d",rdw);
        byte[] personPackeBytes = new byte[rdw];
        ByteBuffer byteBuffer
         = ByteBuffer.wrap(personPackeBytes);
        byteBuffer.put(rdwbytes);
        byteBuffer.put(String.format("%-10s",person.getFname()).getBytes("ibm500"));
        byteBuffer.put(String.format("%-10s",person.getLname()).getBytes("ibm500"));

        PackedDecimal packedDecimal = new PackedDecimal(person.getAmount(),3);
        byte[] amountPackedbytes =  packedDecimal.toByteArray();

        packedDecimal = new PackedDecimal(Integer.parseInt(person.getDob()),5);
        byte[] dobpackedbytes =  packedDecimal.toByteArray();
        byteBuffer.put(dobpackedbytes);
        byteBuffer.put(String.format("%-7s",person.getGender()).getBytes("ibm500"));
        byteBuffer.put(amountPackedbytes);

        return byteBuffer.array();
//        byte[] personBytes = new byte[];


    }
}

package com.test.cobol;

/**
 * Converts between integer and an array of bytes in IBM mainframe packed decimal
 * format. The number of bytes required to store an integer is (digits + 1) / 2.
 * For example, a 7 digit number can be stored in 4 bytes. Each pair of digits is
 * packed into the two nibbles of one byte. The last nibble contains the sign,
 * 0F for positive and 0C for negative. For example 7654321 becomes 0x76 0x54
 * 0x32 0x1F.
 *
 * This class is immutable. Once constructed you can extract the value as an int,
 * an array of bytes but you cannot change the value. Someone should implement
 * equals() and hashcode() to make this thing truly useful.
 */
public class PackedDecimal
{
    private byte[] mBytes;
    private int mInt;
    private static final byte POSITIVE = 0x0f;
    private static final byte NEGATIVE = 0x0c;
    public PackedDecimal(int aInt, int aByteCount)
    {
        mInt = aInt;
        mBytes = new byte[aByteCount];
        int lByteIx = aByteCount - 1;
        // right nibble of first byte is the sign
        if (aInt >= 0)
            mBytes[lByteIx] = POSITIVE;
        else
        {
            mBytes[lByteIx] = NEGATIVE;
            aInt = -aInt;
        }
        // left nibble of the first byte is the ones
        int lDigit = aInt % 10;
        mBytes[lByteIx] |= (lDigit << 4);
        while (lByteIx > 0)
        {
            // next byte over
            lByteIx--;
            // right nibble
            aInt = aInt / 10;
            lDigit = aInt % 10;
            mBytes[lByteIx] |= (lDigit);
            // left nibble
            aInt = aInt / 10;
            lDigit = aInt % 10;
            mBytes[lByteIx] |= (lDigit << 4);
        }
    }
    public PackedDecimal(byte[] aBytes)
    {
        mBytes = aBytes;
        mInt = 0;

        int lByteIx = aBytes.length - 1;
        mInt += aBytes[lByteIx] >> 4;

        int lFactor = 10;
        while (lByteIx > 0)
        {
            lByteIx--;
            mInt += ((aBytes[lByteIx] & 0x0f) * lFactor);
            lFactor *= 10;
            mInt += ((aBytes[lByteIx] >> 4) * lFactor);
            lFactor *= 10;
        }
        if ((aBytes[aBytes.length-1] & 0x0f) == NEGATIVE)
            mInt *= -1;
    }
    public int toInt()
    {
        return mInt;
    }
    public byte[] toByteArray()
    {
        return mBytes;
    }

    public static long parse(byte[] pdIn) throws Exception {
// Convert packed decimal to long
        final int PlusSign = 0x0C;       // Plus sign
        final int MinusSign = 0x0D;      // Minus
        final int NoSign = 0x0F;         // Unsigned
        final int DropHO = 0xFF;         // AND mask to drop HO sign bits
        final int GetLO  = 0x0F;         // Get only LO digit
        long val = 0;                    // Value to return

        for(int i=0; i < pdIn.length; i++) {
            int aByte = pdIn[i] & DropHO; // Get next 2 digits & drop sign bits
            if(i == pdIn.length - 1) {    // last digit?
                int digit = aByte >> 4;    // First get digit
                val = val*10 + digit;
//            System.out.println("digit=" + digit + ", val=" + val);
                int sign = aByte & GetLO;  // now get sign
                if (sign == MinusSign)
                    val = -val;
                else {
                    // Do we care if there is an invalid sign?
                    if(sign != PlusSign && sign != NoSign)
                        throw new Exception("OC7");
                }
            }else {
                int digit = aByte >> 4;    // HO first
                val = val*10 + digit;
//            System.out.println("digit=" + digit + ", val=" + val);
                digit = aByte & GetLO;      // now LO
                val = val*10 + digit;
//            System.out.println("digit=" + digit + ", val=" + val);
            }
        }
        return val;
    } // end parse()
}

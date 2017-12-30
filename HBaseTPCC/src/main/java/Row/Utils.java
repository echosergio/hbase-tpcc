package Row;

import org.apache.hadoop.hbase.util.Bytes;

public final class Utils {

    private static final int INT_SIZE = 4; // Bytes

    public static byte[] getKey(int[] values) {
        byte[] key = new byte[values.length * INT_SIZE];

        int srcPos = 0;
        int destPos = 0;

        for (int value : values) {
            System.arraycopy(Bytes.toBytes(value), srcPos, key, destPos, INT_SIZE);
            destPos += INT_SIZE;
        }

        return key;
    }
}

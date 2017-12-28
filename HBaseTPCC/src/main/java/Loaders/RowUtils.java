package Loaders;

import org.apache.hadoop.hbase.util.Bytes;

public final class RowUtils {

    /**
     * This method generates the key
     *
     * @param values   The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    public static byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable) {
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }
}

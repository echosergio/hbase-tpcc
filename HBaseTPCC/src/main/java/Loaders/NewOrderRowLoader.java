package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class NewOrderRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getFixedKey(new int[] { Integer.parseInt(columns[2]), Integer.parseInt(columns[1]), Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("NO"), Bytes.toBytes("NO_O_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("NO"), Bytes.toBytes("NO_D_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("NO"), Bytes.toBytes("NO_W_ID"), Bytes.toBytes(columns[2]));

        hTable.put(p);
    }

}

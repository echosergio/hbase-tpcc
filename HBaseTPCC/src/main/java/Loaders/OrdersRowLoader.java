package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class OrdersRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getFixedKey(new int[] { Integer.parseInt(columns[2]), Integer.parseInt(columns[1]), Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_D_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_W_ID"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_C_ID"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_ENTRY_D"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_CARRIER_ID"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_OL_CNT"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("O"), Bytes.toBytes("O_ALL_LOCAL"), Bytes.toBytes(columns[7]));

        hTable.put(p);
    }

}

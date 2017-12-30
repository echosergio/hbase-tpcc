package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WarehouseRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = Row.Utils.getKey(new int[] { Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_NAME"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_STREET_1"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_STREET_2"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_CITY"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_STATE"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_ZIP"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_TAX"), Bytes.toBytes(columns[7]));
        p.add(Bytes.toBytes("W"), Bytes.toBytes("W_YTD"), Bytes.toBytes(columns[8]));

        hTable.put(p);
    }

}

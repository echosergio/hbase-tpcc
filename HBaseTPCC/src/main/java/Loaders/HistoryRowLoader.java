package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HistoryRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getKey(new String[] { columns[0], columns[1], columns[2], columns[3] }, new int[] { 0, 1, 2, 3 });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_C_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_C_D_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_C_W_ID"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_D_ID"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_DATE"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_AMOUNT"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("H"), Bytes.toBytes("H_DATA"), Bytes.toBytes(columns[6]));

        hTable.put(p);
    }

}

package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ItemRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = Row.Utils.getKey(new int[] { Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("I"), Bytes.toBytes("I_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("I"), Bytes.toBytes("I_IM_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("I"), Bytes.toBytes("I_NAME"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("I"), Bytes.toBytes("I_PRICE"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("I"), Bytes.toBytes("I_DATA"), Bytes.toBytes(columns[4]));

        hTable.put(p);
    }

}

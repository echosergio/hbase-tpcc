package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DistrictRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getKey(new String[] { columns[0], columns[1] }, new int[] { 1, 0 });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_W_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_NAME"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_STREET_1"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_STREET_2"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_STATE"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_ZIP"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_TAX"), Bytes.toBytes(columns[7]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_YTD"), Bytes.toBytes(columns[8]));
        p.add(Bytes.toBytes("D"), Bytes.toBytes("D_NEXT_O_ID"), Bytes.toBytes(columns[9]));

        hTable.put(p);
    }

}

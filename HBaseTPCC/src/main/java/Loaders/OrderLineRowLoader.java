package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class OrderLineRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getFixedKey(new int[] { Integer.parseInt(columns[2]), Integer.parseInt(columns[1]), Integer.parseInt(columns[0]), Integer.parseInt(columns[3]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_O_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_D_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_W_ID"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_NUMBER"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_I_ID"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_SUPPLY_W_ID"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_DELIVERY_D"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_QUANTITY"), Bytes.toBytes(columns[7]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_AMOUNT"), Bytes.toBytes(columns[8]));
        p.add(Bytes.toBytes("OL"), Bytes.toBytes("OL_DIST_INFO"), Bytes.toBytes(columns[9]));

        hTable.put(p);
    }

}

package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class StockRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = RowUtils.getFixedKey(new int[] { Integer.parseInt(columns[1]), Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_I_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_W_ID,"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_QUANTITY"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_01"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_02"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_03"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_04"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_05"), Bytes.toBytes(columns[7]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_06"), Bytes.toBytes(columns[8]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_07"), Bytes.toBytes(columns[9]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_08"), Bytes.toBytes(columns[10]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_09"), Bytes.toBytes(columns[11]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DIST_10"), Bytes.toBytes(columns[12]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_YTD"), Bytes.toBytes(columns[13]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_ORDER_CNT"), Bytes.toBytes(columns[14]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_REMOTE_CNT"), Bytes.toBytes(columns[15]));
        p.add(Bytes.toBytes("S"), Bytes.toBytes("S_DATA"), Bytes.toBytes(columns[16]));

        hTable.put(p);
    }

}

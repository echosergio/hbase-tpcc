package Loaders;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CustomerRowLoader implements RowLoader {

    @Override
    public void load(HTable hTable, String line) throws IOException {

        String[] columns = line.split(",");

        byte[] rowKey = Row.Utils.getKey(new int[] { Integer.parseInt(columns[1]), Integer.parseInt(columns[2]), Integer.parseInt(columns[0]) });
        Put p = new Put(rowKey);

        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_ID"), Bytes.toBytes(columns[0]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_W_ID"), Bytes.toBytes(columns[1]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_D_ID"), Bytes.toBytes(columns[2]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_FIRST"), Bytes.toBytes(columns[3]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_MIDDLE"), Bytes.toBytes(columns[4]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_LAST"), Bytes.toBytes(columns[5]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_STREET_1"), Bytes.toBytes(columns[6]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_STREET_2"), Bytes.toBytes(columns[7]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_CITY"), Bytes.toBytes(columns[8]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_STATE"), Bytes.toBytes(columns[9]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_ZIP"), Bytes.toBytes(columns[10]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_PHONE"), Bytes.toBytes(columns[11]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_SINCE"), Bytes.toBytes(columns[12]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_CREDIT"), Bytes.toBytes(columns[13]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_CREDITLIM"), Bytes.toBytes(columns[14]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_DISCOUNT"), Bytes.toBytes(columns[15]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_BALANCE"), Bytes.toBytes(columns[16]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_YTD_PAYMENT"), Bytes.toBytes(columns[17]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_PAYMENT_CNT"), Bytes.toBytes(columns[18]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_DELIVERY_CNT"), Bytes.toBytes(columns[19]));
        p.add(Bytes.toBytes("C"), Bytes.toBytes("C_DATA"), Bytes.toBytes(columns[20]));

        hTable.put(p);
    }

}

package Loaders;

import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public interface RowLoader {

    void load(HTable hTable, String line) throws IOException;

}

package Loaders;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.*;
import java.nio.file.Path;

public class TableLoader {

    String tableName;
    Configuration configuration;
    RowLoader rowLoader;

    public TableLoader(String tableName, Configuration configuration, RowLoader rowLoader) {
        this.tableName = tableName;
        this.configuration = configuration;
        this.rowLoader = rowLoader;
    }

    public void loadFile(Path inputFilePath) {
        try{
            InputStream inputFS = new FileInputStream(inputFilePath.toFile());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));

            HTable hTable = new HTable(configuration, tableName);

            br.lines().forEach(line -> {
                try {
                    rowLoader.load(hTable, line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            hTable.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

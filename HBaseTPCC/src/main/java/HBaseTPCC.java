import Loaders.*;
import Row.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseTPCC {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**
     * The Constructor. Establishes the connection with HBase.
     *
     * @param zkHost
     * @throws IOException
     */
    public HBaseTPCC(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    // create 'tableName', 'columnFamilies'
    // alter 'tableName', NAME => 'columnFamily', VERSIONS => versions
    public void createTable(String tableName, String[] columnFamilies, int columnMaxVersions) throws IOException {

        if (!hBaseAdmin.tableExists(tableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String columnFamily : columnFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily).setMaxVersions(columnMaxVersions);
                tableDescriptor.addFamily(hColumnDescriptor);
            }

            hBaseAdmin.createTable(tableDescriptor);
        }
    }

    public void createTPCCTables() throws IOException {

        createTable("Warehouse", new String[] { "W" }, 1);
        createTable("District", new String[] { "D" }, 1);
        createTable("Item", new String[] { "I" }, 1);
        createTable("New_order", new String[] { "NO" }, 1);
        createTable("Orders", new String[] { "O" }, 1);
        createTable("History", new String[] { "H" }, 1);
        createTable("Customer", new String[] { "C" }, 6);
        createTable("Stock", new String[] { "S" }, 1);
        createTable("Order_line", new String[] { "OL" }, 1);

    }

    public void loadTables(String folder) throws IOException {

        new TableLoader("Warehouse", config, new WarehouseRowLoader()).loadFile(Paths.get(folder, "warehouse.csv"));
        new TableLoader("District", config, new DistrictRowLoader()).loadFile(Paths.get(folder, "district.csv"));
        new TableLoader("Item", config, new ItemRowLoader()).loadFile(Paths.get(folder, "item.csv"));
        new TableLoader("New_order", config, new NewOrderRowLoader()).loadFile(Paths.get(folder, "new_order.csv"));
        new TableLoader("Orders", config, new OrdersRowLoader()).loadFile(Paths.get(folder, "orders.csv"));
        new TableLoader("History", config, new HistoryRowLoader()).loadFile(Paths.get(folder, "history.csv"));
        new TableLoader("Customer", config, new CustomerRowLoader()).loadFile(Paths.get(folder, "customer.csv"));
        new TableLoader("Stock", config, new StockRowLoader()).loadFile(Paths.get(folder, "stock.csv"));
        new TableLoader("Order_line", config, new OrderLineRowLoader()).loadFile(Paths.get(folder, "order_line.csv"));

    }

    // scan 'Orders', {FILTER => "PrefixFilter ('warehouseId + districtId') AND (SingleColumnValueFilter('O','O_ENTRY_D',>=,'binary:startDate') AND SingleColumnValueFilter('O','O_ENTRY_D',<,'binary:endDate'))" }
    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {

        HTable hTable = new HTable(config, "Orders");

        FilterList filterList = new FilterList();

        filterList.addFilter(new PrefixFilter(Utils.getKey(new int[] { Integer.parseInt(warehouseId), Integer.parseInt(districtId) })));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("O"), Bytes.toBytes("O_ENTRY_D"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDate)));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("O"), Bytes.toBytes("O_ENTRY_D"), CompareFilter.CompareOp.LESS, Bytes.toBytes(endDate)));

        Scan scan = new Scan();
        scan.setFilter(filterList);

        List<String> customerIds = new ArrayList<>();

        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            customerIds.add(Bytes.toString(result.getValue(Bytes.toBytes("O"), Bytes.toBytes("O_C_ID"))));
        }

        scanner.close();
        hTable.close();

        return customerIds;
    }

    // put 'Customer','warehouseId + districtId + customerId','C:C_DISCOUNT','discount'
    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {

        HTable hTable = new HTable(config, "Customer");

        Put p = new Put(Bytes.toBytes(warehouseId + districtId + customerId));

        for (String discount : discounts) {
            p.add(Bytes.toBytes("C"), Bytes.toBytes("C_DISCOUNT"), Bytes.toBytes(discount));
            hTable.put(p);
        }

        hTable.close();
    }

    // get 'Customer','warehouseId + districtId + customerId', {COLUMN => 'C:C_DISCOUNT', VERSIONS => 4}
    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {

        HTable hTable = new HTable(config, "Customer");

        Get rowGet = new Get(Bytes.toBytes(warehouseId + districtId + customerId));
        rowGet.setMaxVersions(4);

        Result result = hTable.get(rowGet);
        hTable.close();

        return result.getColumnCells(Bytes.toBytes("C"), Bytes.toBytes("C_DISCOUNT")).stream().map(x -> Bytes.toString(CellUtil.cloneValue(x))).toArray(String[]::new);
    }

    // scan 'Customer', {FILTER => "PrefixFilter ('warehouseId + districtId1') OR PrefixFilter ('warehouseId + districtIdN')"}
    public int query4(String warehouseId, String[] districtIds) throws IOException {
        HTable hTable = new HTable(config, "Customer");

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String districtId : districtIds) {
            filterList.addFilter(new PrefixFilter(Utils.getKey(new int[] { Integer.parseInt(warehouseId), Integer.parseInt(districtId) })));
        }

        Scan scan = new Scan();
        scan.setFilter(filterList);

        List<String> customerIds = new ArrayList<>();

        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            customerIds.add(Bytes.toString(result.getValue(Bytes.toBytes("C"), Bytes.toBytes("C_ID"))));
        }

        scanner.close();
        hTable.close();

        return customerIds.size();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTables, loadTables, query1, query2, query3, query4], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTables: csvsFolder.\n " +
                    "\tb) If query1: warehouseId districtId startDate endData.\n  " +
                    "\tc) If query2: warehouseId districtId customerId listOfDiscounts.\n  " +
                    "\td) If query3: warehouseId districtId customerId.\n  " +
                    "\te) If query4: warehouseId listOfdistrictId.\n  ");
            System.exit(-1);
        }

        HBaseTPCC hBaseTPCC = new HBaseTPCC(args[0]);

        if (args[1].toUpperCase().equals("CREATETABLES")) {

            hBaseTPCC.createTPCCTables();

        } else if (args[1].toUpperCase().equals("LOADTABLES")) {

            if (args.length != 3) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            } else if (!(new File(args[2])).isDirectory()) {
                System.out.println("Error: Folder " + args[2] + " does not exist.");
                System.exit(-2);
            }

            hBaseTPCC.loadTables(args[2]);

        } else if (args[1].toUpperCase().equals("QUERY1")) {

            if (args.length != 6) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) warehouseId 4) districtId 5) startDate 6) endData");
                System.exit(-1);
            }

            List<String> customerIds = hBaseTPCC.query1(args[2], args[3], args[4], args[5]);
            System.out.println("There are " + customerIds.size() + " customers that order products from warehouse " + args[2] + " of district " + args[3] + " during after the " + args[4] + " and before the " + args[5] + ".");
            System.out.println("The list of customers is: " + Arrays.toString(customerIds.toArray(new String[customerIds.size()])));

        } else if (args[1].toUpperCase().equals("QUERY2")) {

            if (args.length != 6) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) warehouseId 4) districtId 5) customerId 6) listOfDiscounts");
                System.exit(-1);
            }

            hBaseTPCC.query2(args[2], args[3], args[4], args[5].split(","));

        } else if (args[1].toUpperCase().equals("QUERY3")) {

            if (args.length != 5) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) districtId 5) customerId");
                System.exit(-1);
            }

            String[] discounts = hBaseTPCC.query3(args[2], args[3], args[4]);
            System.out.println("The last 4 discounts obtained from Customer " + args[4] + " of warehouse " + args[2] + " of district " + args[3] + " are: " + Arrays.toString(discounts));

        } else if (args[1].toUpperCase().equals("QUERY4")) {

            if (args.length != 4) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) listOfDistrictIds");
                System.exit(-1);
            }

            System.out.println("There are " + hBaseTPCC.query4(args[2], args[3].split(",")) + " customers that belong to warehouse " + args[2] + " of districts " + args[3] + ".");

        } else {

            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables, query1, query2, query3, query4], 3)Extra parameters for loadTables and queries:" +
                    "a) If loadTables: csvsFolder." +
                    "b) If query1: warehouseId districtId startDate endData" +
                    "c) If query2: warehouseId districtId customerId listOfDiscounts" +
                    "d) If query3: warehouseId districtId customerId " +
                    "e) If query4: warehouseId listOfdistrictId");

            System.exit(-1);
        }

    }

}

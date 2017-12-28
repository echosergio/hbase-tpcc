import Loaders.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.nio.file.Paths;
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

    public void createTable(String tableName, String[] columnFamilies) throws IOException {

        if (!hBaseAdmin.tableExists(tableName)) {

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String columnFamily : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }

            hBaseAdmin.createTable(tableDescriptor);
        }
    }

    public void createTPCCTables() throws IOException {

        createTable("Warehouse", new String[] { "W" });
        createTable("District", new String[] { "D" });
        createTable("Item", new String[] { "I" });
        createTable("New_order", new String[] { "NO" });
        createTable("Orders", new String[] { "O" });
        createTable("History", new String[] { "H" });
        createTable("Customer", new String[] { "C" });
        createTable("Stock", new String[] { "S" });
        createTable("Order_line", new String[] { "OL" });

        System.exit(-1);
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

        System.exit(-1);
    }

    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public int query4(String warehouseId, String[] districtIds) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return 0;
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

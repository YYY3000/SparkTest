import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yinyiyun
 * @date 2018/6/7 15:31
 */
public class HBaseTest {

    /**
     * 创建表
     * @param conf
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    public static void createTable(Configuration conf, String tableName, String[] columnFamily) throws Exception {
        HBaseAdmin admin = (HBaseAdmin) ConnectionFactory.createConnection(conf).getAdmin();

        // todo 2018-6-7 验证表是否存在报错 不验证直接创建可以成功
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " Table exists!");
        } else {
            TableDescriptorBuilder build = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName));

            for (String s : columnFamily) {
                ColumnFamilyDescriptor columnDesc = ColumnFamilyDescriptorBuilder.of(s);
                build.setColumnFamily(columnDesc);
            }

            TableDescriptor tableDesc = build.build();

            admin.createTable(tableDesc);
        }
        admin.close();
    }

    /**
     * 移除表
     * @param conf
     * @param tableName
     * @throws Exception
     */
    public static void dropTable(Configuration conf, String tableName) throws Exception {
        try {
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addRow(Table table, String rowKey, String columnFamily, String key, String value) throws Exception {
        Put rowPut = new Put(Bytes.toBytes(rowKey));
        rowPut.addColumn(columnFamily.getBytes(), key.getBytes(), value.getBytes());
        table.put(rowPut);
    }

    public static void putRow(HTable table, String rowKey, String columnFamily, String key, String value) throws Exception {
        Put rowPut = new Put(Bytes.toBytes(rowKey));
        rowPut.addColumn(columnFamily.getBytes(), key.getBytes(), value.getBytes());
        table.put(rowPut);
    }

    public void addDataBatch(HTable table, List<Put> list) throws Exception {
        table.put(list);
    }

    public void queryAll(HTable table) throws Exception {
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            int i = 0;
            for (Cell cell : result.listCells()) {
                if (i++ == 0) {
                    System.out.print("rowkey:" + new String(cell.toString()) + " ");
                }
            }
            System.out.println();
        }
    }

    public ResultScanner queryBySingleColumn(HTable table, String queryColumn, String value, String[] columns) {
        if (columns == null || queryColumn == null || value == null) {
            return null;
        }

        try {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn), Bytes.toBytes(queryColumn), CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
            Scan scan = new Scan();
            for (String columnName : columns) {
                scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
            }
            scan.setFilter(filter);
            return table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return null;
    }

    public static Result getRow(Table table, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return result;
    }

    public static Map<String, Long> aggregateBySingleColumn(Map<String, String> paramMap, String[] dimensionColumns, String aggregateColumn) {
        if (dimensionColumns == null || dimensionColumns.length == 0 || paramMap == null || aggregateColumn == null || aggregateColumn.equals("")) {
            return null;
        }

        HTable table = null;
        Map<String, Long> map = null;
        try {
            FilterList filterList = new FilterList();
            Scan scan = new Scan();
            for (String paramKey : paramMap.keySet()) {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(paramKey), Bytes.toBytes(paramKey), CompareFilter.CompareOp.EQUAL, new SubstringComparator(paramMap.get(paramKey)));
                filterList.addFilter(filter);
            }
            scan.setFilter(filterList);
            for (String column : dimensionColumns) {
                scan.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
            }
            scan.addColumn(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn));
            ResultScanner results = table.getScanner(scan);
            map = new ConcurrentHashMap<String, Long>();
            for (Result result : results) {
                StringBuilder dimensionKey = new StringBuilder();
                String value = new String(result.getValue(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn)));
                Long aggregateValue = value == null ? 0 : Long.parseLong(value);

                for (String column : dimensionColumns) {
                    dimensionKey.append("\t" + new String(result.getValue(Bytes.toBytes(column), Bytes.toBytes(column))));
                }
                dimensionKey = dimensionKey.deleteCharAt(0);
                if (map.containsKey(dimensionKey)) {
                    map.put(dimensionKey.toString(), map.get(dimensionKey.toString()) + aggregateValue);
                } else {
                    map.put(dimensionKey.toString(), aggregateValue);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("dataTest").master("local").getOrCreate();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://hadoop-mn01:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "192.168.5.169:4180,192.168.5.104:4180,192.168.5.93:4180");
        try {
//            String[] familyColumn = new String[]{"test3", "test4"};
//            createTable(conf, "test", familyColumn);
            dropTable(conf, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

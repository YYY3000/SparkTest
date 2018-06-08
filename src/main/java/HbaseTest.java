import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * @author yinyiyun
 * @date 2018/6/7 18:43
 */
public class HbaseTest {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    /**
     * 初始化hbase链接
     */
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://hadoop-mn01:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "192.168.5.169:4180,192.168.5.104:4180,192.168.5.93:4180");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭hbase链接
     */
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 用hbase接口查询hbase中存在的表
     *
     * @throws IOException
     */
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    private static void readTest(SparkSession spark) {
        init();
        //用hbase接口读取hbase数据
        //把hbase数据读取到
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        try {
            String tableName = "test";
            configuration.set(TableInputFormat.INPUT_TABLE, tableName);

            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(
                    configuration,
                    TableInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class);

            List<Tuple2<ImmutableBytesWritable, Result>> results = myRDD.collect();
            for (Tuple2<ImmutableBytesWritable, Result> result : results) {
                Result rs = result._2;
                String rowkey = Bytes.toString(rs.getRow());
                System.out.println(rowkey);
                List<Cell> cells = rs.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(cell.getFamily()));
                    System.out.println(cell.getTimestamp());
                    System.out.println(Bytes.toString(cell.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeTest(SparkSession spark) throws Exception {
        init();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("word"));
        List<Word> words = new ArrayList<>();
        words.add(new Word(UUID.randomUUID().toString(), "hello", 5));
        words.add(new Word(UUID.randomUUID().toString(), "word", 4));

        JavaRDD<Word> rdd = sc.parallelize(words);
        JavaPairRDD<String, Word> pairRdd = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Word>, String, Word>() {
            @Override
            public Iterator<Tuple2<String, Word>> call(Iterator<Word> words) throws Exception {
                List<Tuple2<String, Word>> pairs = new ArrayList<>();
                while (words.hasNext()) {
                    Word word = words.next();
                    pairs.add(new Tuple2<>(word.getId(), word));
                }
                return pairs.iterator();
            }
        });

        pairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Word>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Word>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Word> tuple2 = tuple2Iterator.next();
                    Put put = new Put(Bytes.toBytes(tuple2._1));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(tuple2._2.getName()));
                    table.put(put);
                }
            }
        });
    }

    public static void main(String[] args) throws IOException {
        try {
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hbase test");
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
            writeTest(spark);
            spark.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

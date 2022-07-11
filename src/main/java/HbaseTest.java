import com.yyy.data.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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

    /**
     * 初始化hbase链接
     */
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://127.0.0.1:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
    }

    private static void readTest(SparkSession spark) {
        init();
        //用hbase接口读取hbase数据
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        try {
            String tableName = "test";
            configuration.set(TableInputFormat.INPUT_TABLE, tableName);

            HBaseUtil hBaseUtil = new HBaseUtil(configuration);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = hBaseUtil.read(sc, tableName, null);

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

        List<Word> words = new ArrayList<>();
        words.add(new Word(UUID.randomUUID().toString(), "hello", 5));
        words.add(new Word(UUID.randomUUID().toString(), "word", 4));

        JavaRDD<Word> rdd = sc.parallelize(words);

        JavaPairRDD<ImmutableBytesWritable, Put> pairRdd = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Word>, ImmutableBytesWritable, Put>() {
            @Override
            public Iterator<Tuple2<ImmutableBytesWritable, Put>> call(Iterator<Word> wordIterator) throws Exception {
                List<Tuple2<ImmutableBytesWritable, Put>> pairs = new ArrayList<>();
                while (wordIterator.hasNext()) {
                    Word word = wordIterator.next();
                    Put put = new Put(Bytes.toBytes(word.getId()));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(word.getName()));
                    put.addColumn(Bytes.toBytes("count"), Bytes.toBytes("name"), Bytes.toBytes(word.getCount()));
                    Tuple2<ImmutableBytesWritable, Put> tuple2 = new Tuple2<>(new ImmutableBytesWritable(), put);
                    pairs.add(tuple2);
                }
                return pairs.iterator();
            }
        });
        HBaseUtil hBaseUtil = new HBaseUtil(configuration);
        hBaseUtil.write(pairRdd, "word");
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

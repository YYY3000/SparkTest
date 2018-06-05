import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yinyiyun
 * @date 2018/6/5 14:33
 */
public class Main {

    public static void main(String[] args) {
        localTest();
        dataTest();
        hbaseTest();
    }

    private static void localTest() {

        SparkConf sparkConf = new SparkConf().setAppName("localTest").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile("F:\\hello.txt", 1);

        JavaPairRDD<String, Integer> words = lines.flatMapToPair(s -> {
            String[] word = s.split(" ", -1);

            List<Tuple2<String, Integer>> pairs = new ArrayList<>();

            for (int ii = 0; ii < word.length; ii++) {
                pairs.add(new Tuple2<>(word[ii], 1));
            }

            return pairs.iterator();
        });

        JavaPairRDD<String, Integer> counts = words.reduceByKey((arg1, arg2) -> arg1 + arg2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    private static void dataTest() {

        // .master 设置spark连接
        SparkSession spark = SparkSession.builder().appName("dataTest").master("local").getOrCreate();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost/test?user=root&password=123456&serverTimezone=UTC")
                .option("dbtable", "word")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .load();
        jdbcDF.show();
        spark.stop();
    }


    private static void hbaseTest() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://hadoop-mn01:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "192.168.5.169:4180,192.168.5.104:4180,192.168.5.93:4180");
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println(tableName);
            }
            connection.close();
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
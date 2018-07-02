package data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;

/**
 * @author yinyiyun
 * @date 2018/7/2 13:40
 */
public class HdfsUtil {


    public static void main(String[] args) throws IOException {
        try {
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hbase test");
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

            String path = "hdfs://192.168.1.16:9000/dxy/testData/20171110/mroout_tianjin/1711102300";
            Dataset<String> data = spark.read().textFile(path);
            data.show();

            spark.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

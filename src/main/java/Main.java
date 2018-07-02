import data.MysqlUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * @author yinyiyun
 * @date 2018/6/5 14:33
 */
public class Main {

    public static void main(String[] args) {
        //localTest();
        //localTest2();
        //dataTest();
        hdfs();
    }

    /**
     * RDD版本单词计数
     */
    private static void localTest() {
        SparkConf sparkConf = new SparkConf().setAppName("localTest").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile("F:\\hello.txt", 1);
        JavaPairRDD<String, Integer> words = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] word = s.split(" ", -1);

                List<Tuple2<String, Integer>> pairs = new ArrayList<>();

                for (int ii = 0; ii < word.length; ii++) {
                    pairs.add(new Tuple2<>(word[ii], 1));
                }

                return pairs.iterator();
            }
        });
        JavaPairRDD<String, Integer> counts = words.reduceByKey((arg1, arg2) -> arg1 + arg2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    /**
     * Dataset版本单词计数
     */
    private static void localTest2() {
        SparkSession spark = SparkSession.builder().appName("localTest").master("local").getOrCreate();
        Dataset<String> lines = spark.read().textFile("F:\\hello.txt");
        Dataset<String> s = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ", -1)).iterator();
            }
        }, Encoders.javaSerialization(String.class));

        Dataset<Tuple2<String, Object>> c = s.groupByKey(new MapFunction<String, String>() {
            @Override
            public String call(String value) throws Exception {
                System.out.println("value===========" + value);
                return value.toLowerCase();
            }
        }, Encoders.javaSerialization(String.class)).count();

        spark.stop();
    }

    private static List<Word> getTestList() {
        List<Word> words = new ArrayList<>();
        words.add(new Word(UUID.randomUUID().toString(), "yinyiyun", 8));
        words.add(new Word(UUID.randomUUID().toString(), "yinsanqian", 10));
        return words;
    }

    private static void dataTest() {

        // .master 设置spark连接
        SparkSession spark = SparkSession.builder().appName("dataTest").master("local").getOrCreate();

        MysqlUtil mysqlUtil = new MysqlUtil("localhost", "3306", "test", "root", "123456");

        // read
        Dataset<Row> dataset = mysqlUtil.readTable(spark, "word");
        dataset.show();

        List<Word> words = getTestList();
        Dataset<Row> data = spark.createDataFrame(words, Word.class);
        data.show();

        //write
        mysqlUtil.writeTable(data, "word_copy");

        spark.stop();
    }

    public static void hdfs() {
        try {
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hbase test");
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

            String path = "hdfs://192.168.1.16:9000/dxy/testData/20171110/mroout_tianjin/1711102300";
            Dataset<String> data = spark.read().textFile(path);
            data.show();

            Dataset<Word> d = data.map(new MapFunction<String, Word>() {
                @Override
                public Word call(String value) throws Exception {
                    String[] values = value.split("\t");
                    for (String s : values) {
                        System.out.println(s);
                    }
                    return new Word(values[0], values[1], Integer.valueOf(values[2]));
                }
            }, Encoders.javaSerialization(Word.class));

            spark.createDataFrame(d.javaRDD(), Word.class).show();

            spark.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

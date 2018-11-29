import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import com.redislabs.provider.redis.RedisEndpoint;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * @author yinyiyun
 * @date 2018/8/15 8:58
 */
public class RedisTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("redis test");
        sparkConf.set("redis.host", "192.168.1.16");
        sparkConf.set("redis.port", "6379");
        sparkConf.set("redis.db", "3");
        sparkConf.set("redis.auth", "mastercom");
        SparkContext sparkContext = new SparkContext(sparkConf);
        RedisContext redisContext = new RedisContext(sparkContext);
        RedisConfig redisConfig = new RedisConfig(new RedisEndpoint(sparkConf));
        RDD<Tuple2<byte[], byte[]>> redisRdd = redisContext.fromRedisByteHash("yyy_*", 5, redisConfig);
        System.out.println(redisRdd.count());
        redisRdd.toJavaRDD().foreach(value -> {
            System.out.println(value._1);
            System.out.println(value._2);
            System.out.println(new String(value._1));
            System.out.println(new String(value._2));
        });
        sparkContext.stop();
    }

}

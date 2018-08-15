import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import com.redislabs.provider.redis.RedisEndpoint;
import com.redislabs.provider.redis.rdd.RedisKeysRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * @author yinyiyun
 * @date 2018/8/15 8:58
 */
public class RedisTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("redis test");
        sparkConf.set("redis.host", "hadoop-dn03");
        sparkConf.set("redis.port", "6379");
        sparkConf.set("redis.auth", "mastercom");
        SparkContext sparkContext = new SparkContext(sparkConf);
        RedisContext redisContext = new RedisContext(sparkContext);
        RedisConfig redisConfig = new RedisConfig(new RedisEndpoint(sparkConf));
        RedisKeysRDD keysRDD = redisContext.fromRedisKeyPattern("sys_*", 5, redisConfig);
        JavaRDD<Tuple2<String, String>> hashRdd = keysRDD.getHash().toJavaRDD();
        System.out.println(hashRdd.count());
        hashRdd.foreach(value -> {
            System.out.println(value._1);
            System.out.println(value._2);
        });
        JavaRDD<String> listRdd =  keysRDD.getList().toJavaRDD();
        System.out.println(listRdd.count());
        listRdd.foreach(value -> {
            System.out.println(value);
        });
        sparkContext.stop();
    }

}

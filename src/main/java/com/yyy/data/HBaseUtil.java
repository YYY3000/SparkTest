package com.yyy.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * HBase 读写工具类
 *
 * @author yinyiyun
 * @date 2018/6/5 18:40
 */
public class HBaseUtil {

    private Configuration configuration;

    public HBaseUtil(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * 读表(全表)
     *
     * @param sc         JavaSparkContext
     * @param tableName  表名
     * @param cacheCount 缓存条数
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable, Result> read(JavaSparkContext sc, String tableName, Integer cacheCount) {
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        if (null != cacheCount) {
            configuration.set(TableInputFormat.SCAN_CACHEDROWS, cacheCount + "");
        }
        return sc.newAPIHadoopRDD(configuration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);
    }

    /**
     * 读表(根据条件读取)
     *
     * @param sc        JavaSparkContext
     * @param tableName 表名
     * @param scan      封装的查询操作参数
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable, Result> read(JavaSparkContext sc, String tableName, Integer cacheRows, Scan scan) throws Exception {
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        if (null != cacheRows) {
            configuration.set(TableInputFormat.SCAN_CACHEDROWS, cacheRows + "");
        }
        if (null != scan) {
            // 加入查询信息
            configuration.set(TableInputFormat.SCAN, convertScanToString(scan));
        }

        return sc.newAPIHadoopRDD(configuration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);
    }

    /**
     * 写数据入表
     *
     * @param rdd       数据RDD
     * @param tableName 表名
     */
    public void write(JavaPairRDD<ImmutableBytesWritable, Put> rdd, String tableName) {
        JobConf jobConf = new JobConf(configuration);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        rdd.saveAsHadoopDataset(jobConf);
    }

    private static String convertScanToString(Scan scan) throws Exception {
        // 将scan类转化成string类型
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanString = Base64.encodeBytes(proto.toByteArray());
        return scanString;
    }

}

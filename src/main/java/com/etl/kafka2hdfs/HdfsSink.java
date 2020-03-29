package com.etl.kafka2hdfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/3/27
 *  @Time: 12:52
 *  
 */
public class HdfsSink {
    public static void main(String[] args) throws Exception {
        String fieldDelimiter = ",";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // checkpoint
        env.enableCheckpointing(10_000);
        //env.setStateBackend((StateBackend) new FsStateBackend("file:///E://checkpoint"));
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://kms-1:8020/checkpoint"));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kms-2:9092,kms-3:9092,kms-4:9092");
        // only required for Kafka 0.8
        props.setProperty("zookeeper.connect", "kms-2:2181,kms-3:2181,kms-4:2181");
        props.setProperty("group.id", "test123");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "qfbap_ods.code_city", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        // transform
        SingleOutputStreamOperator<String> cityDS = stream
                .filter(new FilterFunction<String>() {
                    // 过滤掉DDL操作
                    @Override
                    public boolean filter(String jsonVal) throws Exception {
                        JSONObject record = JSON.parseObject(jsonVal, Feature.OrderedField);
                        return record.getString("isDdl").equals("false");
                    }
                })
                .map(new MapFunction<String, String>() {

                    @Override
                    public String map(String value) throws Exception {
                        StringBuilder fieldsBuilder = new StringBuilder();
                        // 解析JSON数据
                        JSONObject record = JSON.parseObject(value, Feature.OrderedField);

                        // 获取最新的字段值
                        JSONArray data = record.getJSONArray("data");

                        // 遍历，字段值的JSON数组，只有一个元素
                        for (int i = 0; i < data.size(); i++) {

                            // 获取到JSON数组的第i个元素
                            JSONObject obj = data.getJSONObject(i);

                            if (obj != null) {

                                fieldsBuilder.append(record.getLong("id")); // 序号id
                                fieldsBuilder.append(fieldDelimiter); // 字段分隔符
                                fieldsBuilder.append(record.getLong("es")); //业务时间戳
                                fieldsBuilder.append(fieldDelimiter);
                                fieldsBuilder.append(record.getLong("ts")); // 日志时间戳
                                fieldsBuilder.append(fieldDelimiter);
                                fieldsBuilder.append(record.getString("type")); // 操作类型
                                for (Map.Entry<String, Object> entry : obj.entrySet()) {

                                    fieldsBuilder.append(fieldDelimiter);
                                    fieldsBuilder.append(entry.getValue()); // 表字段数据
                                }

                            }
                        }
                        return fieldsBuilder.toString();
                    }

                });

        //cityDS.print();
        //stream.print();

        // sink
        // 以下条件满足其中之一就会滚动生成新的文件
        RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
                .withRolloverInterval(60L * 1000L) //滚动写入新文件的时间，默认60s。根据具体情况调节
                .withMaxPartSize(1024 * 1024 * 128L) //设置每个文件的最大大小 ,默认是128M，这里设置为128M
                .withInactivityInterval(60L * 1000L) //默认60秒,未写入数据处于不活跃状态超时会滚动新文件
                .build();

        StreamingFileSink<String> sink = StreamingFileSink
                //.forRowFormat(new Path("file:///E://binlog_db/city"), new SimpleStringEncoder<String>())
                .forRowFormat(new Path("hdfs://kms-1:8020/binlog_db/code_city_delta"), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000)  // 桶检查间隔，这里设置1S
                .build();

        cityDS.addSink(sink);
        env.execute();
    }
}

package com.etl.kafka2hdfs;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/3/27
 *  @Time: 12:49
 *  
 */

public class EventTimeBucketAssigner implements BucketAssigner<String, String> {


    @Override
    public String getBucketId(String element, Context context) {
        String partitionValue;
        try {
            partitionValue = getPartitionValue(element);
        } catch (Exception e) {
            partitionValue = "00000000";
        }
        return "dt=" + partitionValue;//分区目录名称
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {

        return SimpleVersionedStringSerializer.INSTANCE;
    }

    private String getPartitionValue(String element) throws Exception {

        // 取出最后拼接字符串的es字段值，该值为业务时间
        long eventTime = Long.parseLong(element.split(",")[1]);
        Date eventDate = new Date(eventTime);
        return new SimpleDateFormat("yyyyMMdd").format(eventDate);

    }
}

package com.fashion.kafka.admin.util;

import com.fashion.kafka.admin.constant.QueueServiceType;
import org.junit.Test;

import java.util.Properties;

public class QueueServiceUtilTest {

    @Test
    public void testCreateTopic(){
        Properties props = new Properties();
//        props.put("bootstrap.servers", "192.168.5.134:9092,192.168.5.135:9092");
//        props.put("acks", "all");
//        props.put("retries", "0");
//        props.put("batch.size", "16384");
//        props.put("linger.ms", "1");
//        props.put("buffer.memory", "33554432");
//        props.put("key.serializer",
//                "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer",
//                "org.apache.kafka.common.serialization.StringSerializer");
        QueueServiceUtil.createNewMessageService("kafka-test-3","192.168.5.134:2181,192.168.5.135:2181,192.168.5.136:2181", QueueServiceType.KAFKA,props,3,3);
    }


    @Test
    public void testCommand(){
        String args= "--zookeeper 192.168.5.134:2181 --create --topic kafka-test10 " +
                "  --partitions 3 --replication-factor 1" +
                "  --if-not-exists --config max.message.bytes=204800 --config flush.messages=2";
        QueueServiceUtil.newQueueService(args);

        //bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic test11
    }
}

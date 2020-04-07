package com.fashion.kafka.admin.group;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

public class NumberCustomer {

    public static void main(String[] args) {
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092");
        props.put("group.id", "number");
        props.put("enable.auto.commit", "false");// 禁止自动提交Offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        final KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("number"));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                //释放资源
                consumer.close();
            }
        }));
        while (true) {
            //指定拉取时间，如果超过100ms，就拉取失败
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println("拉==");
//            consumer.beginningOffsets()
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + "--" + record.value());
            }
            consumer.commitSync();
        }
    }
}

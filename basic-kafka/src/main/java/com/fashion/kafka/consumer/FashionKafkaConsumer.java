package com.fashion.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class FashionKafkaConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.128:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "number");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        final KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));


        while (true) {
            //指定拉取时间，如果超过100ms，就拉取失败
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println("拉==");
//            consumer.beginningOffsets()

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + "--" + record.value());
            }
            consumer.commitSync();
            try{
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}

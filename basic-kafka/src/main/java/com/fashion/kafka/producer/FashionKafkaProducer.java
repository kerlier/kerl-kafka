package com.fashion.kafka.producer;

import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class FashionKafkaProducer  {

    private static final String STRING_CLASS_NAME ="ora.apache.kafka.common.serialization.StringSerializer";

    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        Properties properties = new Properties();
        //这里的kafka servers最好指定两个，kafka会根据配置的server连接到集群，假如有一台server,可以通过另外一台继续连接到集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.5.128:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        producer = new KafkaProducer<>(properties);

        Random random = new Random();

        for (int i = 180; i <220 ; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test",i%2,"name"+i,"value"+i);

            try {
                RecordMetadata recordMetadata = producer.send(record).get();
                int partition = recordMetadata.partition();
                System.out.println("name"+i+" 被发送到" + partition);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("aa");
        }

    }


}

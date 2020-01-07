package com.fashion.kafka.producer;

import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public class FashionKafkaProducer  {

    private static final String STRING_CLASS_NAME ="ora.apache.kafka.common.serialization.StringSerializer";

    private static KafkaProducer<String, String> producer;



    public static void main(String[] args) {
        Properties properties = new Properties();
        //这里的kafka servers最好指定两个，kafka会根据配置的server连接到集群，假如有一台server,可以通过另外一台继续连接到集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.5.134:9092,192.168.5.135:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        producer = new KafkaProducer<>(properties);


        int i=0;
        while(true){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-topic", UUID.randomUUID().toString(),"value"+i);

            //同步发送
            producer.send(record);
            i++;
        }

    }


}

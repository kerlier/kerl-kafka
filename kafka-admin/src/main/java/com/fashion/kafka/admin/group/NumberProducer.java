package com.fashion.kafka.admin.group;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class NumberProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //配置kakfa集群的节点，可以是多个
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092");
        props.put("acks", "all");
        props.put("linger.ms", "5");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, NumberPartion.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            //发送消息
            int i1 = random.nextInt(1000);
            String message ="";
            if (i1 % 2 == 0) {
                message ="偶数-"+i1 ;
            }else{
                message ="奇数-"+i1;
            }
            Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<String, String>("number", String.valueOf(i1), message));
            System.out.println("发送" + i);
        }
        //释放资源
        kafkaProducer.close();
    }
}

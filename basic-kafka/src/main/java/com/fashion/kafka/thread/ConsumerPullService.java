package com.fashion.kafka.thread;


import com.fashion.kafka.message.MessageInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 消费message的service
 */
public class ConsumerPullService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPullService.class);

    //同时应该有三个线程消费消息
    private static Integer parallelism = 3;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);

        for (int i = 0; i < parallelism; i++) {
            executor.execute(createRunable(i));
        }
    }

    private static Runnable createRunable(int num){
        return ()->{
            KafkaConsumer kafkaConsumer = initConsumer();

            while(true){

                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                LOGGER.info("poll message, records's size is "+ records.count());
                kafkaConsumer.commitSync();

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("run record, key: "+ record.key() + ", value: "+ record.value());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    LOGGER.info("end to run  record, key: "+ record.key() + ", value: "+ record.value());
                }
            }
        };
    }


    private static KafkaConsumer initConsumer(){
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.134:9092,192.168.5.135:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "number");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        return consumer;
    }
}

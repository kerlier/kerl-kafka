package com.fashion.kafka.admin.commit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * 同步没有提交的消息，第二拉取的时候还是可以被拉取下来
 *
 * commitSync会一直重试，手动提交有一个缺点，就是在没有得到服务器的回应的时候，程序会一直阻塞
 *
 */
public class SyncCommitConsumer {

    private static KafkaConsumer<String,String> consumer;

    private static final String BROKER_SERVER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";

    //kafka在发送信息的时候，如果没有分区器的话，默认使用的轮询的机制，就是依次向每个partition发送消息
    public static void main(String[] args) {

        initConsumer(BROKER_SERVER_URL,"admin-client-consumer");

        consumer.subscribe(Collections.singleton("admin-client-test"));

        while(true){
            ConsumerRecords<String, String> messages = consumer.poll(100);

            for (ConsumerRecord<String, String> message: messages) {
                System.out.println("message key:"+ message.key()+", value: "+ message.value()+", partition: "+ message.partition()+ ", offset: "+message.offset());
            }

            //commitSync 提交的是每个分区的最大offset
            consumer.commitSync();
            try{
                Thread.sleep(2000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    public static void initConsumer(String brokerServerUrl,String groupId){
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServerUrl);
        if(!groupId.isEmpty()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);

    }
}

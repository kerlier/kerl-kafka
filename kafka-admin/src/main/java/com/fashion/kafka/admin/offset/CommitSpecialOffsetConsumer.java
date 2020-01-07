package com.fashion.kafka.admin.offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 提交特定的偏移量： 如果一次poll中拉取上千的消息，此时消息者服务挂掉之后，就会就会出现消息重复消费的情况
 * 所以，需要在处理一条消息之后，就要将这条消息commit掉
 */
public class CommitSpecialOffsetConsumer {

    private static KafkaConsumer<String,String> consumer;

    private static final String BROKER_SERVER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";

    private static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    //kafka在发送信息的时候，如果没有分区器的话，默认使用的轮询的机制，就是依次向每个partition发送消息
    public static void main(String[] args) {

        initConsumer(BROKER_SERVER_URL,"admin-client-consumer");

        //订阅主题
        //subscribe 也可以使用 *.test 的方式
        consumer.subscribe(Collections.singleton("admin-client-test"));

        while(true){
            ConsumerRecords<String, String> messages = consumer.poll(100);

            for (ConsumerRecord<String, String> message: messages) {
                System.out.println("message key:"+ message.key()+", value: "+ message.value()+", partition: "+ message.partition()+ ", offset: "+message.offset());

                offsets.put(new TopicPartition(message.topic(),message.partition()),new OffsetAndMetadata(message.offset()));

                //从特定的偏移量，可以写到for循环中
                consumer.commitSync(offsets);

            }

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
        //consumer默认自动提交数据
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);

    }
}

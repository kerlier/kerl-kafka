package com.fashion.kafka.admin.assignment;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class ConsumerAssignment {

    private static KafkaConsumer<String,String> consumer;

    private static final String BROKER_SERVER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";

    //kafka在发送信息的时候，如果没有分区器的话，默认使用的轮询的机制，就是依次向每个partition发送消息
    public static void main(String[] args) {

        //所以判断当前consumer的currentOffset,只能传入一个当前consumer的实例

        initConsumer(BROKER_SERVER_URL,"admin-client-consumer");

        //订阅主题
        //subscribe 也可以使用 *.test 的方式
        consumer.subscribe(Collections.singleton("admin-client-test"));

        //poll动作触发 分区assign，kafka是懒加载,只有在Poll的时候，才会分配分区
        consumer.poll(0);

        // assignment返回的是 这个consumer分配到的分区
        Set<TopicPartition> assignment = consumer.assignment();

        assignment.stream().forEach(System.out::println);
    }

    public static void initConsumer(String brokerServerUrl,String groupId){
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServerUrl);
        if(!groupId.isEmpty()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        //consumer默认自动提交数据
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);

    }
}

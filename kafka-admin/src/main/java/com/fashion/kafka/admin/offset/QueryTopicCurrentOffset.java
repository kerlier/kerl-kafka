package com.fashion.kafka.admin.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class QueryTopicCurrentOffset {

    private static KafkaConsumer<String,String> consumer;

    private static final String BROKER_SERVER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";

    //kafka在发送信息的时候，如果没有分区器的话，默认使用的轮询的机制，就是依次向每个partition发送消息
    public static void main(String[] args) {

        initConsumer(BROKER_SERVER_URL,"admin-client-consumer");

        //订阅主题
        //subscribe 也可以使用 *.test 的方式
        consumer.subscribe(Collections.singleton("admin-client-test"));

        //listTopic查询的是topic的partition信息
        Map<String, List<PartitionInfo>> topicInfoMap = consumer.listTopics();

        List<PartitionInfo> partitionInfos = topicInfoMap.get("admin-client-test");

        //print
        partitionInfos.stream().forEach(System.out::println);

        List<TopicPartition> topicPartitions = new ArrayList<>();
        //生成TopicPartition
        for (PartitionInfo partitionInfo: partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }

        //endOffset查询是最后一条消息的位置
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(topicPartitions);

        for (Map.Entry<TopicPartition, Long> entry: topicPartitionLongMap.entrySet()) {
            System.out.println("topic: "+entry.getKey().topic()+", partition: "+entry.getKey().partition()+", offset: "+ entry.getValue() );
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
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);

    }
}

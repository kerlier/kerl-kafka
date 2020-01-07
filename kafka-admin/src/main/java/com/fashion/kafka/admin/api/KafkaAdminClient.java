package com.fashion.kafka.admin.api;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * 使用原生的adminClient 对topic进行操作
 */
public class KafkaAdminClient {

    private static final String KAFKA_BROKER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";


    public static void main(String[] args) throws Exception {

        //创建adminClient,需要传入一个broker_url
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_URL);
        AdminClient adminClient = AdminClient.create(properties);

        //列出所有的topic

        //1 listTopicResult 返回一个线程future
        ListTopicsResult listTopicsResult = adminClient.listTopics();

        //listings 返回 future
        KafkaFuture<Collection<TopicListing>> listings = listTopicsResult.listings();

        // future get 返回真正的topic数据
        Collection<TopicListing> topicListings = listings.get();

        topicListings.stream().forEach(System.out::println);

        List<String> topicName = topicListings.stream().map(TopicListing::name).collect(Collectors.toList());

        topicName.stream().forEach(System.out::println);

        //查看描述topic的信息
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicName);
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();

        for (Map.Entry<String, TopicDescription> entry: topicDescriptionMap.entrySet()) {

            System.out.println("topic name:" + entry.getKey());

            System.out.println("topic description:" + entry.getValue());

            //example
            //(name=kafka-test, internal=false,
            // partitions=(partition=0, leader=192.168.5.135:9092 (id: 2 rack: null),
            // replicas=192.168.5.135:9092 (id: 2 rack: null), isr=192.168.5.135:9092 (id: 2 rack: null)))
        }


        //创建topic, 第一个参数是topicName, 第二个参数是分区，第三个参数是副本数
//        NewTopic newTopic = new NewTopic("admin-client-test", 3, (short) 3);

        //创建一个新的list,
//        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));

        //如果topicName已经存在,就会抛出异常  org.apache.kafka.common.errors.TopicExistsException

        // 创建分区的时候，会尽可能将分区分开
//        topics.all().get();
//        (name=admin-client-test, internal=false,
//        partitions=
//        (partition=0, leader=192.168.5.134:9092(id: 0 rack: null),
//                  replicas=192.168.5.134:9092(id: 0 rack: null), 192.168.5.135:9092(id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null),
//                  isr=192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null)),
//        (partition=1, leader=192.168.5.135:9092(id: 2 rack: null),
//                  replicas=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092(id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null),
//                  isr=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092(id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null)),
//        (partition=2, leader=192.168.5.136:9092 (id: 3 rack: null),
//                  replicas=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null)
//                  isr=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null)))


        DescribeClusterResult describeClusterResult = adminClient.describeCluster();

        String clusterId = describeClusterResult.clusterId().get();
        System.out.println("kafka 集群 id: " +clusterId );

        Node node = describeClusterResult.controller().get();
        System.out.println("kafka controller host: "+ node.host() + ", port: "+ node.port());

        Collection<Node> nodes = describeClusterResult.nodes().get();

        nodes.stream().forEach(System.out::println);


        //更改分区
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(6);
        newPartitionsMap.put("admin-client-test",newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(newPartitionsMap);
        partitions.all().get();
        //(name=admin-client-test, internal=false,
        // partitions=
        // (partition=0, leader=192.168.5.134:9092 (id: 0 rack: null),
        //    replicas=192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null),
        //    isr=192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null)),
        // (partition=1, leader=192.168.5.135:9092 (id: 2 rack: null),
        //      replicas=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null),
        //      isr=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null)),
        //  (partition=2, leader=192.168.5.136:9092 (id: 3 rack: null),
        //      replicas=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null),
        //      isr=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.135:9092 (id: 2 rack: null)),
        //  (partition=3, leader=192.168.5.134:9092 (id: 0 rack: null),
        //      replicas=192.168.5.134:9092 (id: 0 rack: null), 192.168.5.136:9092 (id: 3 rack: null), 192.168.5.135:9092 (id: 2 rack: null),
        //      isr=192.168.5.134:9092 (id: 0 rack: null), 192.168.5.136:9092 (id: 3 rack: null), 192.168.5.135:9092 (id: 2 rack: null)),
        //  (partition=4, leader=192.168.5.135:9092 (id: 2 rack: null),
        //      replicas=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.136:9092 (id: 3 rack: null),
        //      isr=192.168.5.135:9092 (id: 2 rack: null), 192.168.5.134:9092 (id: 0 rack: null), 192.168.5.136:9092 (id: 3 rack: null)),
        //   (partition=5, leader=192.168.5.136:9092 (id: 3 rack: null),
        //     replicas=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.135:9092 (id: 2 rack: null), 192.168.5.134:9092 (id: 0 rack: null),
        //     isr=192.168.5.136:9092 (id: 3 rack: null), 192.168.5.135:9092 (id: 2 rack: null), 192.168.5.134:9092 (id: 0 rack: null)))


        adminClient.close();
    }

}

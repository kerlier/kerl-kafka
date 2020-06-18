package com.fashion.kafka.module;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * @author kerlier
 */
public class KafkaApplication {

    private static final String BROKER_SERVER_URL="192.168.5.134:9092";

    /**
     * 初始化kafka consumer,然后设置sessionTimeOut,以及heartbeatTime 必须小于session.time.out 时间
     * @param args
     */
    public static void main(String[] args) {
        new Thread(createRunnable(1)).start();
        new Thread(createRunnable(2)).start();
    }

    public static Runnable createRunnable(int num){
        return ()->{


            KafkaConsumer<String, String> consumer = initConsumer(BROKER_SERVER_URL, "test-rebalance");
            consumer.subscribe(Arrays.asList("kafka-test-4"));

            while(true){
                //assignment 得到这个消费者分配到的分区
                Set<TopicPartition> assignments = consumer.assignment();

                List<String> partitions = new ArrayList<String>();
                for (TopicPartition assignment: assignments) {
                    partitions.add(assignment.partition()+"");
                }
                System.out.println("消费者"+num+"的分区是" +String.join(",",partitions));

                ConsumerRecords<String, String> poll = consumer.poll(100);

                for (ConsumerRecord<String,String > record:
                        poll) {
                    System.out.println(String.join(",",partitions) + " : "+record.value());
                }

                try {
                    //当停顿的时候大于max_pull_interval的时候，此时会触发rebalanced,这个时候commit不能提交
                    Thread.sleep(20000);
                    System.out.println("停顿20s");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        };
    }

    public static KafkaConsumer<String,String> initConsumer(String brokerServerUrl,String groupId){
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServerUrl);
        if(!groupId.isEmpty()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //将session_timeout_ms设置成2000,(2秒)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"6000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"6000");
        //consumer默认自动提交数据
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return  new KafkaConsumer<String, String>(props);
    }
}

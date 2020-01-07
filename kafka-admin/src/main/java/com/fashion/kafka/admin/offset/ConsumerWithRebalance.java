package com.fashion.kafka.admin.offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class ConsumerWithRebalance  {

    private static KafkaConsumer<String,String> consumer;

    private static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    private static final String BROKER_SERVER_URL="192.168.5.134:9092,192.168.5.135:9092,192.168.5.136:9092";

    //kafka在发送信息的时候，如果没有分区器的话，默认使用的轮询的机制，就是依次向每个partition发送消息
    public static void main(String[] args) {

        initConsumer(BROKER_SERVER_URL,"admin-client-consumer");

        //订阅主题
        //subscribe 也可以使用 *.test 的方式
        consumer.subscribe(Collections.singleton("admin-client-test"),new OffsetRebalance(consumer));

       
        while(true){
            ConsumerRecords<String, String> messages = consumer.poll(100);
            for( ConsumerRecord <String, String> message: messages){
                System.out.println("message key:"+ message.key()+", value: "+ message.value()+", partition: "+ message.partition()+ ", offset: "+message.offset());

                offsets.put(new TopicPartition(message.topic(),message.partition()),new OffsetAndMetadata(message.offset()));

                //从特定的偏移量，可以写到for循环中
                consumer.commitSync(offsets);
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
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);

    }

    private static class OffsetRebalance implements ConsumerRebalanceListener{


        private KafkaConsumer<String,String> consumer;

        public OffsetRebalance() {
            super();
        }

        public OffsetRebalance(KafkaConsumer<String,String> consumer){
            this.consumer= consumer;
        }


        /**
         * 持久化分区信息
         * @param collection
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            //先提交当前的offset,然后可以做一些持久化的操作
            this.consumer.commitSync(offsets);

            //TODO 将offset存到数据库中
        }

        /**
         * 读取分区消息
         * @param collection
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            //TODO 从数据库中读取每个分区的offset信息,然后使用seek方法，从特定的位置开始消费
            for (TopicPartition partition:  collection) {
                OffsetAndMetadata offsetAndMetadata = offsets.get(partition);
                this.consumer.seek(partition,offsetAndMetadata.offset());
            }

        }
    }

}

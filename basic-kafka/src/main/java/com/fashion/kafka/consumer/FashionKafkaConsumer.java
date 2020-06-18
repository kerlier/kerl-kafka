package com.fashion.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FashionKafkaConsumer {
    public static void main(String[] args) {


        Jedis jedis = new Jedis("192.168.198.72", 6379);
        jedis.auth("kerl");
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.198.72:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"6000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"2000");
        final KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        Map<TopicPartition, Long> offsetMap = new HashMap<>();

        consumer.subscribe(Arrays.asList("test-topic"), new ConsumerRebalanceListener(){

            /**
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("rebalance before");
                partitions.forEach(x->
                        {
                            String key= "test"+ x.partition();
                            Long partitionOffset = offsetMap.get(x);
                            jedis.hset(key,"offset",String.valueOf(partitionOffset));
                            System.out.println("partition: "+ x+", offset:" + partitionOffset);
                        }
                );
                System.out.println("rebalance after");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(x->{
                            String key= "test"+ x.partition();
                            String offset = jedis.hget(key, "offset");
                            System.out.println("partition: "+ x+", offset:" + (offset+1));
                            Long offset2 =0l;
                            if(Objects.nonNull(offset)){
                                offset2 = Long.parseLong(offset)+1;
                            }
                            consumer.seek(x,offset2);
                        }
                );
            }
        });



        while (true) {
            //指定拉取时间，如果超过100ms，就拉取失败
            ConsumerRecords<String, String> records = consumer.poll(100);
//            consumer.beginningOffsets()
            Set<TopicPartition> assignment = consumer.assignment();

//            System.out.println("begin to pause....");
//            consumer.pause(assignment);
//            System.out.println("pasued:" + consumer.paused());
            for (TopicPartition partition : assignment) {

                String key= "test"+ partition.partition();
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                partitionRecords.forEach(x->{
                    System.out.println("partition:"+x.partition()+",key:"+x.key() + ",value:" + x.value());
                    offsetMap.put(partition,x.offset());
                    jedis.hset(key,"offset",String.valueOf(x.offset()));
//                    try {
//                        TimeUnit.MINUTES.sleep(1l);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                 }
                );

            }
            //            System.out.println("wait 20 seconds");
//            try {
//                TimeUnit.SECONDS.sleep(20l);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("end to pause....");
//            System.out.println("before:" + consumer.paused());
//            System.out.println("after:" + consumer.paused());
//            try{
//                Thread.sleep(1000);
//            }catch (Exception e){
//                e.printStackTrace();
//            }

        }
    }
}

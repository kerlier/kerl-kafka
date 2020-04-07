package com.fashion.kafka.admin.commit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 异步提交
 * 异步提交不会重试,异步提交可以设置一个回调函数
 */
public class AsyncCommitConsumer {
    private static String COMMIT_FLAG= "";

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
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {

                    //判断是否发生异常
                    if(!Objects.isNull(e)){ //如果e不为空，说明发生异常
                           System.out.println(" 异步提交发生异常");
                        String commitFlag = getCommitFlag(map.values());
                        //如果commitFlag一致情况的情况，才能重试提交
                        if(Objects.equals(commitFlag,COMMIT_FLAG)){
                            consumer.commitAsync();
                        }
                    }else{//如果没有发生异常，flag+1
                        COMMIT_FLAG = getCommitFlag(map.values());
                    }
                }
            });


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

    private static String getCommitFlag(Collection<OffsetAndMetadata > offsets){
        StringBuffer stringBuffer = new StringBuffer();

        for (OffsetAndMetadata value: offsets) {
            long offset = value.offset();
            stringBuffer.append(offset).append("-");
        }

        return stringBuffer.toString();
    }

}

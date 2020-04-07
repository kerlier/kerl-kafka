package com.fashion.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaRunnable  implements  Runnable{

    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    private KafkaConsumer consumer ;

    private Map<String,Boolean> statusMap;
    private int i;

    public KafkaRunnable(int i){
        statusMap = new HashMap<>();
        Properties props = new Properties();
        //配置kafka服务器的ip地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.128:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "number");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// 禁止自动提交Offset
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");// 禁止自动提交Offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));
        this.i=i;
    }

    @Override
    public void run() {

        String currentMessage = "";
        while (true){

            System.out.println(i+ "拉取消息中....");
            ConsumerRecords<String, String> records = consumer.poll(100);
            Set assignment = consumer.assignment();
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(i +"消费消息" +  record.key() + "--" + record.value());

                if(Objects.equals(record.key(),"name200")){
                    consumer.pause(assignment);
                    executor.submit(new OwnThread(record.key()));

                    currentMessage = record.key();

                    System.out.println(i+"处理数据中，"+record.key()+ " 需要处理10分钟");
                }
            }

            boolean b = checkJobStatus(currentMessage);

            if(b){
                System.out.println(i+"resume");
                consumer.commitSync();
                consumer.resume(assignment);
            }

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(i+ "拉取消息中....");
        }
    }

    private boolean checkJobStatus(String jodId){
        Boolean aBoolean = statusMap.get(jodId);
        if(Objects.nonNull(aBoolean)&&aBoolean.booleanValue()){
            return  true;
        }
        return false;
    }

    public class OwnThread implements  Runnable
    {

        private String message;

        public OwnThread(String message){
             this.message =message;
        }

        @Override
        public void run() {

            System.out.println("处理"+ message);
            try {
                TimeUnit.MINUTES.sleep(10);
            }catch (Exception e){
                e.printStackTrace();
            }
            statusMap.put(message,true);
            System.out.println("处理成功"+ message);
        }
    }
}

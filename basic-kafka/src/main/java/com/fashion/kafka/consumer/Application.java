package com.fashion.kafka.consumer;

public class Application {

    public static void main(String[] args) {

        new Thread(new KafkaRunnable(1)).start();

        new Thread(new KafkaRunnable(2)).start();
    }
}

package com.fashion.kafka.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskPool {

    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

}

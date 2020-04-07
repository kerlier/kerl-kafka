package com.fashion.kafka.admin.balance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * 消费者重新负载均衡
 */
public class HandleRebalance implements ConsumerRebalanceListener {

    /**
     * revoke 无效
     *  这个方法是在分区负载均衡之前，消费者停止读取消息之后被调用
     * @param collection
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("持久化分区信息");
    }

    /**
     * assign 分配
     * 这个是在分区被分配之后，消费者读取消息之前被调用
     * @param collection
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println("读取分区信息");
    }
}

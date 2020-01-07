package com.fashion.kafka.admin.util;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public class QueueServiceUtil {

    public static void createNewMessageService(String topic, String hosts, String serviceType, Properties props, int partition, int replica) {
        ZkUtils utils = null;
        ZkClient zkClient = null;
        try {
            //当中这两个参数是timeOut时间
            zkClient = new ZkClient(hosts, 30000, 30000, ZKStringSerializer$.MODULE$);
            utils = new ZkUtils(zkClient, new ZkConnection(hosts), false);
            if (!AdminUtils.topicExists(utils, topic)) { //如果这个主题不存在，就创建主题
                AdminUtils.createTopic(utils, topic, partition, replica, props, AdminUtils.createTopic$default$6());
                System.out.println("主题创建成功");
            } else {
                System.out.println("主题已经存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != utils) {
                utils.close();
            }
            if (null != zkClient) {
                zkClient.close();
            }
        }
    }

    public static void newQueueService(String args) {
        String[] configs = args.split(" ");
        System.out.println(configs.toString());
        TopicCommand.main(configs);
        System.out.println("创建成功");
    }

}

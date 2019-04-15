package com.spark.kafka.javaApi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
/*
   https://blog.csdn.net/cjf_wei/article/details/78075263
 */
public class NewKafkaReciveUtil {
    public static void reciveMsg(String brokerList, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", "group1");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<String, String>(properties);
            //订阅主题列表topic 支持多个topic
            consumer.subscribe(Arrays.asList(topic));

            while (true){
//                通过循环的调用poll方法来从partition中获取消息
                ConsumerRecords<String,String> records = consumer.poll(100);
                for (ConsumerRecord<String,String> record:records){
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()+"\n");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        String  brokerList = "node1:9092,node2:9092:node3:9092";
        String topic = "test";

        reciveMsg(brokerList,topic);
    }

}

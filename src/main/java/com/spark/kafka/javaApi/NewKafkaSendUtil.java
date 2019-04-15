package com.spark.kafka.javaApi;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by dell on 2018/6/18.
 * https://blog.csdn.net/ljheee/article/details/81569748
 * https://blog.csdn.net/cjf_wei/article/details/77920435
 *
 * 生产者 消费者
 * https://blog.csdn.net/m0_38075425/article/details/81357833
 */

/***
查看所有topic
kafka-topics --list --zookeeper node1:2181,node2:2181,node3:2181


创建topic，一个备份，1个分区
kafka-topics --create --zookeeper node1:2181,node2:2181,node3:2181 --replication-factor 1 --partitions 6 --topic test


向topic中添加内容
kafka-console-producer --broker-list node1:9002,node2:9092,node3:9092 --topic test
 ***/

public class NewKafkaSendUtil implements Serializable {

    public static void sendMsg(String brokerList, String topic, List<MsgBean> datas) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);

        //将 key value 序列化  消费者接收数据后要反序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = null;
        try {
            producer = new KafkaProducer(properties);
            for (int i = 0; i < datas.size(); i++) {
                // 接收三个参数 topic  key value  其中key可以省略  都是String类型
                String key = String.valueOf(datas.get(i).getId());
                String value = JSON.toJSONString(datas.get(i));


                ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, key, value);

                // 同步发送
                 producer.send(message);


                //异步发送  可以充分利用回调函数和异步发送方式来确认消息发送的进度:
//                producer.send(message, new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata metadata, Exception e) {
//                        if(e != null) {
//                            e.printStackTrace();
//                        } else {
//                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
//                        }
//                    }
//                });





                System.out.println("发送消息到kafka服务器 message:" + JSON.toJSONString(datas.get(i)));
                Thread.sleep(1000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }

        //获取指定topic的partition元数据信息   分区数是创建的时候指定的
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        partitions = producer.partitionsFor(topic);
        for (PartitionInfo p : partitions) {
            System.out.println(p);
        }

        System.out.println("send message over.");
    }

    public static void main(String[] args) {
        String brokerList = "node1:9092,node2:9092:node3:9092";
        List<MsgBean> datas = new ArrayList<>();

        int msgNo = 1;
        final int msgCount = 10;

        while (msgNo < msgCount) {
            MsgBean msgBean = new MsgBean();
            msgBean.setId(msgNo);
            msgBean.setMsg("中午好");

            datas.add(msgBean);
            msgNo++;
        }
        String topic = "test";

        sendMsg(brokerList, topic, datas);
    }

}
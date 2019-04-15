package com.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProducerTest {
    private static final String[] WORDS = {
            "hello", "hadoop", "java", "kafka", "spark"
    };

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092:node3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(props);
        boolean flag = true;
        while (flag) {
            for (int i = 0; i < 500; i++) {
                //3、发送数据
                kafkaProducer.send(new ProducerRecord("test", WORDS[new Random().nextInt(5)]));
            }
            kafkaProducer.flush();
            System.out.println("==========Kafka Flush==========");
            Thread.sleep(5000);
        }

        kafkaProducer.close();
    }
}

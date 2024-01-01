package com.example.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerAsyncWithCustomCallback {

    static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithCustomCallback.class);
    public static void main(String[] args) throws InterruptedException {

        String topicName = "multipart-topic";
        Properties props = new Properties();

        // bootstrap.servers
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "192.168.64.4:9092");

        // key.serializer.class
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        // value.serializer.class
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducerÎ
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq,"hello async world " + seq);
            // kafkaProducer message send with callback!!
            kafkaProducer.send(producerRecord, new CustomCallback(seq));
        }

        Thread.sleep(3000L);
    }
}

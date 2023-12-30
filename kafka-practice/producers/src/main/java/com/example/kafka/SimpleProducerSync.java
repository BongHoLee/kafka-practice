package com.example.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerSync {

    static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);
    public static void main(String[] args) {

        String topicName = "simple-topic";
        Properties props = new Properties();

        // bootstrap.servers
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "192.168.64.4:9092");

        // key.serializer.class
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // value.serializer.class
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducerÎ
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 2");

        // kafkaProducer message send
        try (kafkaProducer) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ##### record metadata received #### \n");
            logger.info("partition: {}", recordMetadata.partition());
            logger.info("offset: {}", recordMetadata.offset());
            logger.info("timestamp: {}", recordMetadata.timestamp());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // producing은 사실 batch로 수행되기 때문에 버퍼 잔여를 모두 내보내기 위해 flush
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

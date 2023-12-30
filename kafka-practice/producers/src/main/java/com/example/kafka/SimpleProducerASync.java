package com.example.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerASync {

    static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class);
    public static void main(String[] args) throws InterruptedException {

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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello async world ");

        // kafkaProducer message send with callback!!
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                StringBuilder message = new StringBuilder("\n##### record metadata received async ####\n");
                message.append("partition: ").append(recordMetadata.partition()).append("\n");
                message.append("offset : ").append(recordMetadata.partition()).append("\n");
                message.append("timestamp: ").append(recordMetadata.timestamp()).append("\n");

                logger.info(message.toString());
            } else {
                logger.error("exception occur from broker " + exception.getMessage(), exception);
            }
        });


        Thread.sleep(3000L);
    }
}

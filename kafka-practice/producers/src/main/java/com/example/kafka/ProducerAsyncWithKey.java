package com.example.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerAsyncWithKey {

    static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class);
    public static void main(String[] args) throws InterruptedException {

        String topicName = "multipart-topic";
        Properties props = new Properties();

        // bootstrap.servers
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "192.168.64.4:9092");

        // key.serializer.class
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // value.serializer.class
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducerÎ
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq),"hello async world " + seq);
            // kafkaProducer message send with callback!!
            int finalSeq = seq;
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    StringBuilder message = new StringBuilder("\n##### record metadata received async ####\n");
                    message.append("seq: ").append(finalSeq).append("\n");
                    message.append("partition: ").append(recordMetadata.partition()).append("\n");
                    message.append("offset : ").append(recordMetadata.offset()).append("\n");
                    message.append("timestamp: ").append(recordMetadata.timestamp()).append("\n");

                    logger.info(message.toString());
                } else {
                    logger.error("exception occur from broker " + exception.getMessage(), exception);
                }
            });
        }



        Thread.sleep(3000L);
    }
}

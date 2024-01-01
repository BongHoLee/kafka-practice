package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final int sequence;

    public CustomCallback(int sequence) {
        this.sequence = sequence;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            logger.info("seq: {} partition: {} offset: {}", this.sequence, recordMetadata.partition(), recordMetadata.offset());
        } else {
            logger.error("exception occur from broker " + exception.getMessage(), exception);
        }

    }
}

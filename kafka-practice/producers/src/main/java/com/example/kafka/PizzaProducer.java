package com.example.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PizzaProducer {
    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);
    private static final String BOOT_STRAP_SERVER = "192.168.64.4:9092";
    private static final String KEY = "key";
    private static final String MESSAGE = "message";

    private final KafkaProducer<String, String> kafkaProducer;

    public PizzaProducer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void sendPizzaMessage(String topicName, int iterCount, int interIntervalMillis, int intervalMillis,
                                 int intervalCount, SendMessageStrategy messageStrategy) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        Random random = new Random(2024);
        long startTime = System.currentTimeMillis();

        while (iterSeq++ != iterCount) {
            Map<String, String> pMessage = pizzaMessage.produce_msg(random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get(KEY), pMessage.get(MESSAGE));

            try {
                messageStrategy.run(kafkaProducer, producerRecord);
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (intervalCount > 0 && iterSeq % intervalCount == 0) {
                try {
                    logger.info("####### IntervalCount: {} intervalMillis: {} #########", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis: {}" , interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        logger.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);
    }

    private enum SendMessageStrategy {
        SYNC((producer, producerRecord) -> {
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(producerRecord).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("error ", e);
                throw new RuntimeException(e);
            }
            logger.info("sync message: {} partition: {} offset: {}", producerRecord.value(), recordMetadata.partition(), recordMetadata.offset());
        }),
        ASYNC((producer, producerRecord) ->
            producer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: {} partition: {}  offset:{}", producerRecord.value(), recordMetadata.partition(), recordMetadata.offset());
                } else {
                    logger.error("exception error from broker " + exception.getMessage(), exception);
                }
            })
        );

        private StrategyFunction function;

        SendMessageStrategy(StrategyFunction function) {
            this.function = function;
        }

        public void run(KafkaProducer<String, String> producer, ProducerRecord<String, String> record)
                throws ExecutionException, InterruptedException {
            this.function.apply(producer, record);
        }

        @FunctionalInterface
        private interface StrategyFunction {
            void apply(KafkaProducer<String, String> producer, ProducerRecord<String, String> record);
        }
    }


    public static void main(String[] args) throws InterruptedException {

        PizzaProducer pizzaProducer = new PizzaProducer();
        String topicName = "pizza-topic";

        pizzaProducer.sendPizzaMessage(topicName, -1, 10, 100, 100, SendMessageStrategy.ASYNC);
    }
}

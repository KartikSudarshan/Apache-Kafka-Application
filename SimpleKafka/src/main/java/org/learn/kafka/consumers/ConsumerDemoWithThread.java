package org.learn.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {


    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();


    }

    private ConsumerDemoWithThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        String bootstrapServers = "10.88.36.82:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating Consumer Thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();


        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application inturruped", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        logger.info("Key: " + consumerRecord.key() + " | Value: " + consumerRecord.value());
                        logger.info("Partition: " + consumerRecord.partition() + " | Offset: " + consumerRecord.offset());
                    }
                }
            } catch (WakeupException wakeup) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                //tell main code that we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //special method to interrupt consumer.poll()
            //it will throw WakeUpException
            consumer.wakeup();

        }
    }
}

package org.learn.kafka.producers;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.Properties;

public class ProducerDemoKeys {
    /*
    * Concept: Kafka will always put the same key to the same partition based on the hashing algorithm
    *
    * Following may vary Depending on where the Kafka Machine is setup
    * Key_1 <=> Partition: 1
    * Key_2 Partition: 2
    * Key_3 Partition: 0
    * Key_4 Partition: 2
    * Key_5 Partition: 2
    * Key_6 Partition: 1
    * Key_7 Partition: 1
    * Key_8 Partition: 0
    * Key_9 Partition: 0
    * */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create producer properties, Old way of hardcoding
        Logger logger= LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

        Properties properties =new Properties();
        String bootstrapServers="localhost:9092";
        /*
        properties.setProperty("bootstrap.servers",bootstrapServers);
        properties.setProperty("key.serializer",StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        String topic="first_topic";


        //Create producer record
        for (int i=0;i<10;i++) {
            String value=ProducerDemoKeys.class.getName()+": Hello World "+Integer.toString(i);
            String key="Key_"+Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);
            logger.info("Key: "+key);
            //Send Data -- asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime when record is successfully sent or Exception is thrown
                    if (e == null) {
                        //record was sent successfully
                        logger.info("Received new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while Producing");
                        e.printStackTrace();

                    }
                }
            }).get(); // block send to make it synchronous only to understand concept
        }
        producer.flush();
        producer.close();
    }

}

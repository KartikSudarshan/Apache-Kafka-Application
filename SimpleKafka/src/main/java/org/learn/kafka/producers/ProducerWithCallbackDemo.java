package org.learn.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {
    public static void main(String[] args) {
        // Create producer properties, Old way of hardcoding
        Logger logger= LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getName());

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


        //Create producer record
        for (int i=0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", ProducerWithCallbackDemo.class.getName()+": hello world "+Integer.toString(i));

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
            });
        }
        producer.flush();
        producer.close();
    }
}

package org.learn.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // Create producer properties, Old way of hardcoding
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
        ProducerRecord<String,String> record= new ProducerRecord<String,String>("first_topic",ProducerDemo.class.getName()+": hello world");
        ProducerRecord<String,String> record2= new ProducerRecord<String,String>("first_topic",ProducerDemo.class.getName()+": This is a new message I am writing");

        //Send Data -- asynchronous
        producer.send(record);producer.send(record2);
        producer.flush();
        producer.close();

    }
}

package com.github.subhendu.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
public class ProducerDemo {
    public static void main(String[] args){
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer producer=new KafkaProducer(properties);

        //Create a producer record
        ProducerRecord record=new ProducerRecord("first_topic","Hello Subhendu!");

        //send data
        producer.send(record);
        producer.flush();
        producer.close();
    }
}

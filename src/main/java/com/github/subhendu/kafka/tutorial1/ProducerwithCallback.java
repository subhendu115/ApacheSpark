package com.github.subhendu.kafka.tutorial1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerwithCallback {
    public static void main(String[] args){
        Logger logger=LoggerFactory.getLogger(ProducerwithCallback.class);
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer producer=new KafkaProducer(properties);

        //Create a producer record
        ProducerRecord record=new ProducerRecord("first_topic","Hello Subhendu. This is Producer with callback!");

        //send data
        producer.send(record,new Callback(){
            @java.lang.Override
            public void onCompletion(org.apache.kafka.clients.producer.RecordMetadata recordMetadata, java.lang.Exception e) {
                if (e==null){
                    logger.info("Received new metadata. \n"+
                            "Topic:"+recordMetadata.topic()+"\n"+
                            "Partition:"+recordMetadata.partition()+"\n"+
                            "Offset:"+recordMetadata.offset()+"\n"+
                            "Timestamp:"+recordMetadata.timestamp());

                }
                else{
                    logger.error("Error - ",e);

                }
            }
        });
        producer.flush();
        producer.close();
    }
}

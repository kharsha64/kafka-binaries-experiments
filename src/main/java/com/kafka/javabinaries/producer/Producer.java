package com.kafka.javabinaries.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic_two", "test_message2");

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                log.info("Metadata Information- \n" +
                        "Topic -" + recordMetadata.topic() + " \n" +
                        "Offset -" + recordMetadata.offset() + " \n" +
                        "Partition -" + recordMetadata.partition());
            } else {
                log.error(" Error - {0}", e);
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

}

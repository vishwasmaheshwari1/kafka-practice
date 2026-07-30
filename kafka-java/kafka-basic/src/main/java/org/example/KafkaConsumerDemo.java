package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());

    public static void main(String[] args) {

        log.info("This is Kafka consumer");

        String groupId = "java-app";
        String topic = "demo_java";

        // producer props
        Properties props = new Properties();
//        props.setProperty("key", "value");
        props.setProperty("bootstrap.servers", "localhost:9092");

        // create consumer config
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll - pull data from topic
        while (true) {
            log.info("polling");
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }

    }
}
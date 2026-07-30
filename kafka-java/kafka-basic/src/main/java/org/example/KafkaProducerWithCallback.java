package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWithCallback.class.getName());

    public static void main(String[] args) {

        log.info("his is Kafka producer with callback api");

        // producer props
        Properties props = new Properties();
//        props.setProperty("key", "value");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());


        props.setProperty("batch.size", "400");
//        props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 30; i++) {

                // create producer record and send data
                ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world " + i + j);

                // asynchronously
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes if record is sent or exception is thrown
                        if (exception == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" + "Topic:" + metadata.topic() + "\n" + "Partition: " + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });

            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        // flush and close the producer
        //to send data to send all data and block unit done - synchronous
        producer.flush();
        producer.close();
    }
}
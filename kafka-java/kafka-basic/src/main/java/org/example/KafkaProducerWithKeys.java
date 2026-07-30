package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWithKeys.class.getName());

    public static void main(String[] args) {

        log.info("his is Kafka producer with callback api");

        // producer props
        Properties props = new Properties();
//        props.setProperty("key", "value");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());


        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                // create producer record and send data
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // asynchronously
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes if record is sent or exception is thrown
                        if (exception == null) {
                            // the record was successfully sent
                            log.info("Key:" + key + " | Partition: " + metadata.partition());
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
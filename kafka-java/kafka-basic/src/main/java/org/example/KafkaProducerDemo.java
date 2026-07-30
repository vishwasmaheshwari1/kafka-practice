package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerDemo.class.getName());

    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        log.info("his is Kafka producer");

//        for (int i = 1; i <= 5; i++) {
//            //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon
//            // src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
//            // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
//            System.out.println("i = " + i);
//        }

        // producer props
        Properties props = new Properties();
//        props.setProperty("key", "value");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        // create producer record and send data
        ProducerRecord<String,String> record = new ProducerRecord<>("demo_java", "hello world");

        // asynchronously
        producer.send(record);

        // flush and close the producer
        //to send data to send all data and block unit done - synchronous
        producer.flush();
        producer.close();
    }
}
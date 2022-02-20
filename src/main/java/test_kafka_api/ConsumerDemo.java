package test_kafka_api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "third_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("first_topic"));
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerWorker worker = new ConsumerWorker(consumer, latch);

        Thread consumerThread = new Thread(worker);
        consumerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.shutdown();
            try {
                latch.await();
            } catch (InterruptedException ie) {
                System.out.println("Application got interrupted !!!");
            } finally {
                System.out.println("Application killed !!!");
            }
        }));
//        try {
//            latch.await();
//        } catch (InterruptedException ie) {
//            System.out.println("Application closing !!!");
//        } finally {
//            System.out.println("Application killed !!!");
//        }
    }
}

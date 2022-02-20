import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class ConsumerWorker implements Runnable {
    private KafkaConsumer<String,String> consumer;
    private CountDownLatch latch;

    public ConsumerWorker(KafkaConsumer<String,String> consumer, CountDownLatch latch) {
        this.consumer = consumer;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
                record.forEach((cr) -> {
                    System.out.println(cr.key() + " | " + cr.value());
                });
            }
        } catch(WakeupException we){
            System.out.println("Wakeup exception");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}

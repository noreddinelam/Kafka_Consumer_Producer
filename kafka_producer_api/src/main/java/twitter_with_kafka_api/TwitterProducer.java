package twitter_with_kafka_api;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static twitter_with_kafka_api.TwitterApiConstants.*;

public class TwitterProducer {

    private final static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        /** Create kafka Producer */
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        /** Add shutdown hook */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.stop();
            kafkaProducer.close();
        }));

        String msg = "";
        while (!twitterClient.isDone()) {
            try {
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    logger.info("Received Message {}", msg);
                    kafkaProducer.send(new ProducerRecord<>("tweeter_topic", null, msg), (re, e) -> {
                        if (e != null)
                            logger.error("Error while sending tweet", e);
                    });
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
                kafkaProducer.close();
            }
        }
    }

    /**
     * Create Twitter client
     */
    public static Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hoseBirdEndpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hoseBirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);
        ClientBuilder builder = new ClientBuilder()
                .name("HoseBird-Client-01")
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /** Add some of advanced properties */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");// By Default.
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // the number of messages
        // to send without ACK.

        /** Enhance producer efficiency */
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        return new KafkaProducer<>(properties);
    }
}

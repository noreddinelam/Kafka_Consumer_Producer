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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
        String msg = "";
        while (!twitterClient.isDone()) {
            try {
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
                if (msg != null)
                    logger.info("Received Message {}", msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
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
        return new ClientBuilder()
                .name("HoseBird-Client-01")
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue)).build();
    }
}

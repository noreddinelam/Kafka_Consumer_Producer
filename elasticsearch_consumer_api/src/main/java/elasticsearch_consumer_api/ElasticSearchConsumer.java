package elasticsearch_consumer_api;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    private final static String username = "ew4rijy55k";
    private final static String password = "qrd6figt0q";
    private final static String hostname = "twitter-consumer-6421897892.eu-west-1.bonsaisearch.net";

    private final static String consumerGroup = "consumer-group";
    private final static String bootstrapServer = "localhost:9092";
    private final static String kafkaTopic = "tweeter_topic";

    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(hostname, 443, "https"))
                        .setHttpClientConfigCallback((httpClientBuilder) ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        );

        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            KafkaConsumer<String, String> consumer = createConsumer(kafkaTopic, consumerGroup, bootstrapServer);
            BulkRequest bulkRequest = new BulkRequest();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                int recordCounts = records.count();
                logger.info("{} records have been read",recordCounts);
                for (ConsumerRecord<String, String> record : records) {
                    IndexRequest indexRequest = new IndexRequest("tweeter");
                    bulkRequest.add(indexRequest.source(record.value(), XContentType.JSON));
                    Thread.sleep(10);
                }
                if(recordCounts > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing offsets");
                    consumer.commitSync();// commit offsets synchronously
                    Thread.sleep(1000);
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.info("There is an error on the code !");
        }
    }

    public static KafkaConsumer<String, String> createConsumer(String topic, String consumerGroup,
                                                               String bootstrapServer) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit offset.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // fix the max size of the buffer where we read data from.

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        return kafkaConsumer;
    }
}

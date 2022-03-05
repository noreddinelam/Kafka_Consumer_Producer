package elasticsearch_consumer_api;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Properties;

public class ElasticSearchConsumer {
    private final static String username = "ew4rijy55k";
    private final static String password = "qrd6figt0q";
    private final static String hostname = "twitter-consumer-6421897892.eu-west-1.bonsaisearch.net";

    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(hostname, 443, "https"))
                        .setHttpClientConfigCallback((httpClientBuilder) ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        );

        String jsonString = "{\"foo\":\"bar\"}";

        try(RestHighLevelClient client = new RestHighLevelClient(builder)) {
            IndexRequest indexRequest = new IndexRequest("twitter").source(jsonString, XContentType.JSON);
            IndexResponse indexResponse =  client.index(indexRequest, RequestOptions.DEFAULT);
            String id = indexResponse.getId();
            logger.info("Index response succeeded with id {}",id);
        } catch (IOException e) {
            logger.info("There is an error on the code !");
        }
    }

    public static KafkaConsumer<String,String> createConsumer(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "")
        return new KafkaConsumer<>(properties);
    }
}

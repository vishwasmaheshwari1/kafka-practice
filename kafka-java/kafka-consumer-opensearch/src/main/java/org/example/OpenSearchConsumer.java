package org.example;

import com.google.gson.JsonParser;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;

import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient;
import org.opensearch.client.opensearch.generic.Requests;
import org.opensearch.client.opensearch.generic.Response;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {

        System.out.printf("OpenSearch Consumer!");
        String indexName = "wikimedia";
        String topic = "wikimedia.recentchange";

        // open search client
        OpenSearchClient openSearchClient = createOpenSearchClient();

        // create kafka consumer
        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        // for graceful shutdown

        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown. let's exist by calling consumer.wakeup()....");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        // creating index on opensearch if doesnt exist
        try {
            // 2. Check if the index already exists
            boolean indexExists = openSearchClient.indices().exists(ExistsRequest.of(b -> b.index(indexName))).value();

            if (!indexExists) {
                // 3. Create the index using the strongly-typed builder lambda syntax
                CreateIndexResponse createIndexResponse = openSearchClient.indices().create(b -> b.index(indexName));

                if (createIndexResponse.acknowledged()) {
                    System.out.println("Index '" + indexName + "' successfully created!");
                }
            } else {
                System.out.println("Index '" + indexName + "' already exists. Skipping creation.");
            }


            // send records
            OpenSearchGenericClient genericClient = openSearchClient.generic();

            consumer.subscribe(Collections.singleton(topic));

            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordsCount = records.count();
                log.info("Record Count: " + recordsCount);

                // another way to improve performance to reduce network calls
//                BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();

                for (ConsumerRecord<String,String> record : records) {

                    // send the record in opensearch
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                    // Strategy to geenrate id to make process idempotent
                    // and not commit values in opensearch twice
                    //1.
                    String id = "";
                    id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    //2.
                    id = extractId(record.value());

                    try {

                        // 2. Perform a direct REST request using standard endpoint formatting
                        Response response = genericClient.execute(
                                Requests.builder()
                                        .endpoint("/" + indexName + "/_doc/" + id)
                                        .method("POST")
                                        .json(record.value()) // <--- Natively accepts your raw JSON string!
                                        .build()
                        );

                        // System.out.println("HTTP Status Code: " + response.getStatus());
                        if (response.getBody().isPresent()) {
                            System.out.println("Response Body: " + response.getBody().get().bodyAsString());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // commit offset after batch [records] is consumed [atleast once scenario]
                consumer.commitSync();
                log.info("Offsets have been manually committed!");
            }

        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (IOException e) {
            System.err.println("Error interacting with OpenSearch cluster: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            // 4. Properly close the openSearchClient resources
            closeClient(openSearchClient);
        }

    }

    private static String extractId(String json) {
        // gson
        return JsonParser.parseString(json)
                .getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-opensearch";
//        String topic = "";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // auto-commit offsets
        // note: consumer group offset can be changed according to how much data we want to reprocess
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }

    private static void closeClient(OpenSearchClient client) {
        if (client != null) {
            try {
                // OpenSearchClient itself doesn't implement AutoCloseable,
                // you must fetch and close its underlying transport layer.
                OpenSearchTransport transport = client._transport();
                if (transport != null) {
                    transport.close();
                    System.out.println("OpenSearch client transport closed successfully.");
                }
            } catch (IOException e) {
                System.err.println("Failed to close OpenSearch client transport: " + e.getMessage());
            }
        }
    }
    public static OpenSearchClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        // String connString = "https://bonsaisearch.net";

        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(connUri.getScheme(), connUri.getHost(), connUri.getPort())
        );

        if (userInfo != null) {
            // Extract and configure credentials
            String[] auth = userInfo.split(":");
            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(auth[0],auth[1].toCharArray()));

            builder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
            );
        }

        // Build the underlying low-level RestClient
        RestClient restClient = builder.build();

        // Create the transport wrapper using a JSON mapper
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        // Return the modern OpenSearchClient instance
        return new OpenSearchClient(transport);
    }
}
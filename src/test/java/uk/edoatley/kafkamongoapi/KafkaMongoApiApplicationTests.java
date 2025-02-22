package uk.edoatley.kafkamongoapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.edoatley.kafkamongoapi.model.Movie;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {KafkaMongoApiApplicationTests.TEST_TOPIC}, ports = 9092)
class KafkaMongoApiApplicationTests {
    public static final String TEST_TOPIC = "movie.test";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void contextLoads() {
    }

    @Test
    void pingEndpointReturnsPong() {
        ResponseEntity<String> response = restTemplate.getForEntity("/ping", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("pong");
    }

    @Test
    @SneakyThrows
    void aMovieSentToAddMovieEndpointAppearsOnTheKafkaTopic() {
        Movie newMovie = new Movie();
        newMovie.setId("1");
        newMovie.setTitle("Test Movie");
        newMovie.setDirector("John Doe");
        newMovie.setReleaseYear(2023);
        String movieString = objectMapper.writeValueAsString(newMovie);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", this.embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<Integer, String> consumer = cf.createConsumer();
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TEST_TOPIC);

        ResponseEntity<Object> response = restTemplate.exchange("/movie", HttpMethod.POST, new HttpEntity<>(newMovie), Object.class, Map.of());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Verify that a message was sent to Kafka
        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        await().untilAsserted(() -> assertThat(records.count()).isEqualTo(1));
        ConsumerRecord<Integer, String> firstRecord = records.iterator().next();
        assertThat(firstRecord.value()).isEqualTo(movieString);

    }



}

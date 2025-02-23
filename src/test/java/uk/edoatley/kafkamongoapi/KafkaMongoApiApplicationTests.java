package uk.edoatley.kafkamongoapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.AutoConfigureDataMongo;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import uk.edoatley.kafkamongoapi.model.Movie;
import uk.edoatley.kafkamongoapi.repository.MovieRepository;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
@AutoConfigureDataMongo
@EmbeddedKafka(topics = {KafkaMongoApiApplicationTests.TEST_TOPIC})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KafkaMongoApiApplicationTests {
    public static final String TEST_TOPIC = "movie.test";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MovieRepository movieRepository;

    @BeforeEach
    void init() {
        movieRepository.deleteAll();
    }

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
        // Given
        Movie newMovie = newMovie();
        String movieString = objectMapper.writeValueAsString(newMovie);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", this.embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<Integer, String> consumer = cf.createConsumer();
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TEST_TOPIC);

        ResponseEntity<Object> response = restTemplate.exchange("/movie", HttpMethod.POST, new HttpEntity<>(newMovie), Object.class, Map.of());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Verify that a message was sent to Kafka
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            // Validate the message appears in the Kafka topic
            ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10_000));
            assertThat(records).isNotEmpty();
            List<String> recordValueList = extractValuesFromRecords(records);
            System.err.println(recordValueList);
            assertThat(recordValueList).contains(movieString);
        });
    }

    private List<String> extractValuesFromRecords(ConsumerRecords<Integer, String> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value)
                .toList();
    }

    @Test
    @SneakyThrows
    void aMessageSentToKafkaIsSavedToMongoDB() {
        // Given
        Movie newMovie = newMovie();

        // when
        ResponseEntity<Object> response = restTemplate.exchange("/movie", HttpMethod.POST, new HttpEntity<>(newMovie), Object.class, Map.of());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        // then
        await().untilAsserted(() -> assertThat(movieRepository.count()).isEqualTo(1));
        Optional<Movie> movieFromDb = movieRepository.findById(newMovie.getId());
        assertThat(movieFromDb).isPresent();
        assertThat(movieFromDb.get().getTitle()).isEqualTo("Kafka to MongoDB Test");
        assertThat(movieFromDb.get().getDirector()).isEqualTo("Jane Doe");
        assertThat(movieFromDb.get().getReleaseYear()).isEqualTo(2023);
    }

    private Movie newMovie() {
        Movie newMovie = new Movie();
        newMovie.setId(UUID.randomUUID().toString());
        newMovie.setTitle("Kafka to MongoDB Test");
        newMovie.setDirector("Jane Doe");
        newMovie.setReleaseYear(2023);
        return newMovie;
    }

}

package uk.edoatley.kafkamongoapi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import uk.edoatley.kafkamongoapi.model.Movie;
import uk.edoatley.kafkamongoapi.repository.MovieRepository;

@Service
@RequiredArgsConstructor
@Slf4j
public class MovieKafkaConsumerService {

    private final ObjectMapper objectMapper;
    private final MovieRepository movieRepository;

    @KafkaListener(topics = "movie.test", groupId = "movieGroup", containerFactory = "kafkaListenerContainerFactory")
    public void consumeMovie(ConsumerRecord<Integer, String> record) {
        try {
            // Deserialize the message to a Movie object
            Movie movie = objectMapper.readValue(record.value(), Movie.class);
            
            // Save the movie to MongoDB
            movieRepository.save(movie);
            log.info("Saved movie to MongoDB: {}", movie);
        } catch (Exception e) {
            log.error("Failed to process Kafka message: {}", record.value(), e);
        }
    }
}
package uk.edoatley.kafkamongoapi.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import uk.edoatley.kafkamongoapi.model.Movie;

public interface MovieRepository extends MongoRepository<Movie, String> {
}


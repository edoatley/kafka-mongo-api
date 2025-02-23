package uk.edoatley.kafkamongoapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.edoatley.kafkamongoapi.repository.MovieRepository;

@SpringBootApplication
public class KafkaMongoApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaMongoApiApplication.class, args);
    }

}

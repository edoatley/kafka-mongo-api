package uk.edoatley.kafkamongoapi.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    private static final int PARTITIONS = 1;
    private static final short REPLICATION_FACTOR = 1;

    @Bean
    public String movieTopicName(@Value("${app.movie.topic}") String topicName) {
        return topicName;
    }

    @Bean
    public NewTopic myTopic(@Value("${app.movie.topic}") String topicName) {
        return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }
}
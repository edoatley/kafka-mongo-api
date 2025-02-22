package uk.edoatley.kafkamongoapi.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String movieTopicName;

    public void sendMessage(String movie) {
        kafkaTemplate.send(movieTopicName, movie);
    }
}
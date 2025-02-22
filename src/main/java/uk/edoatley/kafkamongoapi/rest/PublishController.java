package uk.edoatley.kafkamongoapi.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import uk.edoatley.kafkamongoapi.model.Movie;
import uk.edoatley.kafkamongoapi.service.PublisherService;

@RestController
@RequiredArgsConstructor
public class PublishController {

    private final ObjectMapper mapper;
    private final PublisherService publisherService;

    @GetMapping("/ping")
    public ResponseEntity<String> ping() {
        return ResponseEntity.ok("pong");
    }

    @PostMapping("/movie")
    public ResponseEntity<?> addMovie(@RequestBody Movie newMovie) {
        try {
            String message = mapper.writeValueAsString(newMovie);
            publisherService.sendMessage(message);
            return ResponseEntity.ok(newMovie); // Respond with the movie received.
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Failed to process the movie: " + e.getMessage());
        }
    }

}

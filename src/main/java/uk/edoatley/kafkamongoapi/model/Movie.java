package uk.edoatley.kafkamongoapi.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Movie {
    private String id;
    private String title;
    private String director;
    private int releaseYear;
}

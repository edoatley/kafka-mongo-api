package uk.edoatley.kafkamongoapi.repository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.edoatley.kafkamongoapi.model.Movie;

import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles("test")
@DataMongoTest()
@ExtendWith(SpringExtension.class)
class MovieRepositoryTest {

    @Autowired
    private MovieRepository movieRepository;

    @Test
    void testSaveMovie() {
        // given
        Movie newMovie = new Movie();
        newMovie.setId("1");
        newMovie.setTitle("Test Movie");
        newMovie.setDirector("John Doe");
        newMovie.setReleaseYear(2023);
        movieRepository.deleteAll();

        // when
        movieRepository.save(newMovie);

        // then
        assertNotNull(movieRepository.findById(newMovie.getId()));
    }
}
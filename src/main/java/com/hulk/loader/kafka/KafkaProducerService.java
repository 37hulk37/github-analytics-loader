package com.hulk.loader.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hulk.loader.RepositoryBasicDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic}")
    private String topic;

    public void sendAsync(List<RepositoryBasicDto> dtos) {
        dtos.stream()
            .map(dto -> Pair.of(dto.getFullName(), mapToJson(dto)))
            .forEach(pair ->
                kafkaTemplate.send(topic, pair.getFirst(), pair.getSecond())
                    .whenComplete((res, throwable) -> {
                        if (throwable != null) {
                            handleError(pair.getFirst(), throwable);
                        }
                    })
            );
    }

    private String mapToJson(RepositoryBasicDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            log.error("Error while writing repositories as json", e);
            throw new RuntimeException(e);
        }
    }

    private void handleError(String repoFullName, Throwable throwable) {
        log.error("Could not send a repository {}", repoFullName, throwable);
    }

}

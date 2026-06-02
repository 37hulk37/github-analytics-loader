package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import com.hulk.loader.kafka.KafkaProducerService;
import com.hulk.loader.minio.MinioService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@StepScope
@RequiredArgsConstructor
@Slf4j
public class GithubWriter implements ItemWriter<RepositoryBasicDto> {

    @Value("#{jobParameters['searchDate']}")
    private String searchDate;

    private final MinioService minioService;
    private final KafkaProducerService kafkaProducerService;

    @Override
    public void write(Chunk<? extends RepositoryBasicDto> chunk) throws Exception {
        var list = chunk.getItems()
            .stream()
            .map(it -> (RepositoryBasicDto) it)
            .toList();

        log.trace("chunk size: {}", list.size());

        minioService.writeToMinio(list, searchDate);
        kafkaProducerService.sendAsync(list);
    }
}

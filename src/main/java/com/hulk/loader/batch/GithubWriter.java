package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import com.hulk.loader.kafka.KafkaProducerService;
import com.hulk.loader.minio.MinioService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@StepScope
@RequiredArgsConstructor
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

        minioService.writeToMinio(list, searchDate);
        kafkaProducerService.sendAsync(list);
    }
}

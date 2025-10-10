package com.hulk.loader.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hulk.loader.LocalStorageManager;
import com.hulk.loader.RepositoryBasicDto;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@StepScope
@RequiredArgsConstructor
@Component
public class MinioItemWriter implements ItemWriter<RepositoryBasicDto> {
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final LocalStorageManager localStorageManager;

    @Value("${app.minio.bucket-name}")
    private String bucket;

    @Value("${app.local.save-always}")
    private boolean saveAlways;

    @Value("#{jobParameters['searchDate']}")
    private String searchDate;

    @Override
    public void write(Chunk<? extends RepositoryBasicDto> chunk) throws Exception {

        var items = chunk.getItems();
        String repoString;
        try {
            repoString = objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            log.error("Error while writing repositories as json", e);
            throw new RuntimeException(e);
        }

        var inputStream = new ByteArrayInputStream(repoString.getBytes(StandardCharsets.UTF_8));
        String fileName = searchDate + "_" + UUID.randomUUID().toString() + ".json";
        try {
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(fileName)
                    .stream(inputStream, inputStream.available(), -1)
                    .build()
            );
        } catch (Exception e) {
            log.warn("Could not save data to minio, will try to save data locally", e);
            throw e;
        }

        if (saveAlways) {
            localStorageManager.store(fileName, repoString.getBytes(StandardCharsets.UTF_8));
        }

        log.info("Successfully saved file: {}", fileName);
    }
}

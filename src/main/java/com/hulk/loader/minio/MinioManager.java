package com.hulk.loader.minio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hulk.loader.LocalStorageManager;
import com.hulk.loader.RepositoryBasicDto;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
@RequiredArgsConstructor
public class MinioManager implements InitializingBean {
    private final MinioProps props;
    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final LocalStorageManager localStorageManager;

    @Value("${app.local.save-always}")
    private boolean alwaysSaveLocally;

    private final Map<String, List<RepositoryBasicDto>> repoMap = new ConcurrentHashMap<>();

    public void saveRepository(String key, GHRepository repository) throws IOException {
        var repoList = repoMap.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));

        List<RepositoryBasicDto> repositoriesToSave;

        synchronized (repoList) {
            repoList.add(RepositoryBasicDto.fromGHRepository(repository));
            if (repoList.size() >= props.recordsPerFile()) {
                var listView = repoList.subList(0, props.recordsPerFile());
                repositoriesToSave = new ArrayList<>(listView);
                saveRepositoriesToMinio(key, repositoriesToSave);
                listView.clear();
            }
        }

    }

    public void freeKey(String key) {
        var list = repoMap.get(key);

        if (list == null) {
            log.warn("No repositories found for key {}", key);
            return;
        }

        synchronized (list) {
            var listCopy = new ArrayList<>(list);
            saveRepositoriesToMinio(key, listCopy);
            list.clear();
        }

        repoMap.remove(key);
    }

    private void saveRepositoriesToMinio(String key, List<RepositoryBasicDto> repositories) {
        String repoString;
        try {
            repoString = objectMapper.writeValueAsString(repositories);
        } catch (JsonProcessingException e) {
            log.error("Error while writing repositories as json", e);
            throw new RuntimeException(e);
        }

        var inputStream = new ByteArrayInputStream(repoString.getBytes(StandardCharsets.UTF_8));
        String fileName = key + "_" + UUID.randomUUID().toString() + ".json";
        boolean successMinioSave = false;

        try {
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(props.bucketName())
                    .object(fileName)
                    .stream(inputStream, inputStream.available(), -1)
                    .build()
            );
            successMinioSave = true;
        } catch (Exception e) {
            log.warn("Could not save data to minio, will try to save data locally", e);
        }

        if (!successMinioSave || alwaysSaveLocally) {
            localStorageManager.store(fileName, repoString.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        boolean exists;
        try {
            exists = minioClient.bucketExists(
                BucketExistsArgs
                    .builder()
                    .bucket(props.bucketName())
                    .build()
            );
        } catch (Exception e) {
            log.error("Could not connect to Minio", e);
            throw e;
        }
        if (exists) {
            log.info("Bucket already exists, skip creating a bucket");
            return;
        }
        log.info("Initializing bucket with name: {}", props.bucketName());
        try {
            minioClient.makeBucket(
                MakeBucketArgs.builder()
                    .bucket(props.bucketName())
                    .build()
            );
        } catch (Exception e) {
            log.error("Could not create bucket", e);
            throw e;
        }
        log.info("Bucket was successfully created");
    }
}

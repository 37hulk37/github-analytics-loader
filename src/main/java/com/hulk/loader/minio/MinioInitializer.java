package com.hulk.loader.minio;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MinioInitializer implements InitializingBean {
    private final MinioClient minioClient;
    private final MinioProps props;

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

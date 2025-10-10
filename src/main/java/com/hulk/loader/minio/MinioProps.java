package com.hulk.loader.minio;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.minio")
public record MinioProps(
    String username,
    String password,
    String url,
    String bucketName,
    Integer recordsPerFile
) {
}
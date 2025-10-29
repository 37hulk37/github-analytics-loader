package com.hulk.loader.configuration;

import com.hulk.loader.minio.MinioProps;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class MinioConfig {

    @Bean
    public MinioClient minioClient(MinioProps props) {
        return MinioClient.builder()
            .endpoint(props.url())
            .credentials(props.username(), props.password())
            .build();
    }
}

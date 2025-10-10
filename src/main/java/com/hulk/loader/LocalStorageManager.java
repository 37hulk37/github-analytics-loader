package com.hulk.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
@RequiredArgsConstructor
@Slf4j
public class LocalStorageManager {
    @Value("${app.local.directory}")
    private String localDirectory;

    public void store(String name, byte[] data) {
        try {
            Path dir = Paths.get(localDirectory);
            Files.createDirectories(dir);
            Path filePath = dir.resolve(name);

            Files.write(filePath, data);
        } catch (Exception e) {
            log.error("Could not store data locally", e);
        }
    }
}

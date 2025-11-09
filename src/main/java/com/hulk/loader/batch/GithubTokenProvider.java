package com.hulk.loader.batch;

import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubAbuseLimitHandler;
import org.kohsuke.github.GitHubBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class GithubTokenProvider {

    private final List<GitHub> clients;
    private final AtomicInteger currentIndex = new AtomicInteger(0);

    public GithubTokenProvider(
        @Value("${app.api-keys:}") String apiKeysCsv,
        @Value("${app.api-key:}") String singleApiKey
    ) throws IOException {
        List<String> prepared = new ArrayList<>();

        if (apiKeysCsv != null && !apiKeysCsv.isBlank()) {
            for (String raw : apiKeysCsv.split(",")) {
                String token = raw.trim();
                if (!token.isEmpty()) {
                    prepared.add(token);
                }
            }
        }

        if (prepared.isEmpty() && singleApiKey != null && !singleApiKey.isBlank()) {
            prepared.add(singleApiKey.trim());
        }

        if (prepared.isEmpty()) {
            log.warn("No GitHub API keys provided. Requests will be unauthenticated and heavily rate-limited.");
        }

        List<GitHub> initializedClients = new ArrayList<>();
        if (prepared.isEmpty()) {
            initializedClients.add(GitHubBuilder.fromEnvironment().build());
        } else {
            for (String token : prepared) {
                GitHub client = new GitHubBuilder()
                    .withOAuthToken(token)
                    .withAbuseLimitHandler(GitHubAbuseLimitHandler.WAIT)
                    .build();
                initializedClients.add(client);
            }
            log.info("Initialized GitHub token provider with {} client(s).", initializedClients.size());
        }

        this.clients = Collections.unmodifiableList(initializedClients);
    }

    public synchronized GitHub nextClient() {
        if (clients.isEmpty()) {
            throw new IllegalStateException("No GitHub clients available");
        }

        int index = Math.floorMod(currentIndex.getAndIncrement(), clients.size());
        return clients.get(index);
    }
}



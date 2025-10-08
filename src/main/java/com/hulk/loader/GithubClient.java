package com.hulk.loader;

import com.hulk.loader.minio.MinioManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedSearchIterable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Slf4j
public class GithubClient implements InitializingBean {
    private GitHub gitHub;
    private final MinioManager minioManager;

    @Value("${app.api-key}")
    private String apiKey;

    private final String testDate = "2024-01-01"; //todo: make a queue

    public PagedSearchIterable<GHRepository> searchData() {
        return gitHub.searchRepositories()
            .created(testDate)
            .list()
            .withPageSize(100);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //todo: make several accounts and download data in several threads and move it from 'afterPropertiesSet'
        //todo: make counting of available api calls (we have only 5k for client)
        gitHub = new GitHubBuilder().withOAuthToken(apiKey).build();
        var searchResult = searchData();

        int count = 0;

        for (var repo : searchResult) {
            minioManager.saveRepository(testDate, repo);
            count++;
            if (count > 200) { //todo: just for test, remove later
                minioManager.freeKey(testDate);
                break;
            }
        }

        log.info("All data was fetched");
    }
}

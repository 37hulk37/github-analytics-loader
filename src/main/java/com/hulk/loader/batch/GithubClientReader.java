package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedSearchIterable;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@RequiredArgsConstructor
@Slf4j
@StepScope
@Component
public class GithubClientReader implements ItemReader<RepositoryBasicDto> {

    private GitHub gitHub; // token rotation via GithubTokenProvider
    private Iterator<GHRepository> repositoryIterator;

    private final GithubTokenProvider tokenProvider;

    @Value("#{jobParameters['searchDate']}")
    private String searchDate;

    private boolean dataInitialized = false;

    @Override
    public RepositoryBasicDto read() throws Exception {
        if (!dataInitialized) {
            initializeData();
            dataInitialized = true;
        }

        if (repositoryIterator != null && repositoryIterator.hasNext()) {
            var repo = repositoryIterator.next();
            log.debug("Add repo {}", repo.getFullName());
            return RepositoryBasicDto.fromGHRepository(repo, -1);
        }

        return null;
    }

    private void initializeData() throws Exception {
        this.gitHub = tokenProvider.nextClient();

        if (searchDate == null) {
            throw new UnexpectedInputException("searchDate is required");
        }
        PagedSearchIterable<GHRepository> searchResult = gitHub.searchRepositories()
            .created(searchDate)
            .list()
            .withPageSize(100);

        this.repositoryIterator = searchResult.iterator();

        log.info("GitHub data initialized for date: {}", searchDate);
    }
}
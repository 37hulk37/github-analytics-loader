package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHException;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.PagedSearchIterable;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

@RequiredArgsConstructor
@Slf4j
@StepScope
@Component
public class GithubClientReader implements ItemReader<RepositoryBasicDto> {

    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int githubSearchMax = 1000;
    private static final int maxRetry = 10;

    private Iterator<GHRepository> repositoryIterator;

    private final GithubTokenProvider tokenProvider;

    @Value("#{jobParameters['searchDate']}")
    private String searchDate;

    private LocalTime currentTime = LocalTime.of(0, 0);
    private int secondDelta = 300;

    private boolean searchCompleted = false;

    @Override
    public RepositoryBasicDto read() throws Exception {
        if(searchCompleted) {
            return null;
        }

        if (repositoryIterator != null && repositoryIterator.hasNext()) {
            var repo = repositoryIterator.next();
            log.trace("Add repo {}", repo.getFullName());
            return RepositoryBasicDto.fromGHRepository(repo, -1);
        }

        if (currentTime.isBefore(LocalTime.MAX)) {
            fetchNextSearch();
            if (repositoryIterator != null && repositoryIterator.hasNext()) {
                var repo = repositoryIterator.next();
                log.trace("Add repo {}", repo.getFullName());
                return RepositoryBasicDto.fromGHRepository(repo, -1);
            }
        }
        searchCompleted = true;
        return null;
    }

    private void fetchNextSearch() throws Exception {
        int retry = 0;

        if (searchDate == null) {
            throw new UnexpectedInputException("searchDate is required");
        }

        PagedSearchIterable<GHRepository> iterator = null;
        while (iterator == null) {
            try {
                iterator = getNextIterable();
            } catch (GHException e) {
                log.warn("Got an API limit for date {}", searchDate);
                try {
                    Thread.sleep(retry * 1_000L);
                } catch (InterruptedException ex) {
                    log.error(ex.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
            retry++;
            if (retry >= maxRetry) {
                log.error("infinite cycle in while loop");
                this.repositoryIterator = null;
            }
        }
        this.repositoryIterator = iterator.iterator();
    }

    private PagedSearchIterable<GHRepository> getNextIterable() {
        var gitHub = tokenProvider.nextClient();

        var startTime = currentTime;
        var endTime = currentTime.plusSeconds(secondDelta);
        if (endTime.isBefore(startTime)) {
            endTime = LocalTime.MAX;
        }

        var searchString = String.format("%sT%s..%sT%s", searchDate, timeFormatter.format(startTime), searchDate, timeFormatter.format(endTime));
        PagedSearchIterable<GHRepository> searchResult = gitHub.searchRepositories()
            .created(searchString)
            .list()
            .withPageSize(100);

        if(searchResult.getTotalCount() > githubSearchMax && secondDelta > 1) {
            secondDelta = secondDelta / 2;
            return null;
        }

        if(searchResult.getTotalCount() < githubSearchMax / 3) {
            secondDelta = secondDelta * 3;
        }

        log.info("Github data initialized for date: {} and time: {} with search query result size: {}", searchDate, timeFormatter.format(currentTime), searchResult.getTotalCount());

        currentTime = endTime;
        return searchResult;
    }
}
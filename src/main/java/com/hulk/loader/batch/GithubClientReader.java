package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import jakarta.annotation.PostConstruct;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

@RequiredArgsConstructor
@Slf4j
@StepScope
@Component
public class GithubClientReader implements ItemReader<RepositoryBasicDto> {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int githubSearchMax = 1000;
    private static final int maxRetry = 10;
    private static final int minSecondDelta = 5;

    private Iterator<GHRepository> repositoryIterator;

    private final GithubTokenProvider tokenProvider;

    @Value("#{jobParameters['searchDate']}")
    private String searchDatetime;
    private String searchDate;
    private LocalDateTime searchEnd;

    private LocalDateTime currentTime;
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

        if (currentTime.isBefore(searchEnd)) {
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

    @PostConstruct
    public void postConstruct() {
        var searchStart = LocalDateTime.parse(searchDatetime, dateTimeFormatter);
        searchDate = searchStart.format(dateFormatter);
        searchEnd = searchStart.plusHours(1);
        currentTime = searchStart.plusSeconds(0);
    }

    private void fetchNextSearch() throws Exception {
        int retry = 0;

        if (searchDatetime == null) {
            throw new UnexpectedInputException("searchDate is required");
        }

        PagedSearchIterable<GHRepository> iterator = null;
        while (iterator == null) {
            try {
                iterator = getNextIterable();
            } catch (GHException e) {
                log.warn("Got an API limit for date {}", searchDatetime);
                try {
                    Thread.sleep(retry * 1_000L);
                } catch (InterruptedException ex) {
                    log.error(ex.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
            retry++;
            if (retry >= maxRetry) {
                log.error("infinite cycle in while loop, for date {}", searchDatetime);
                this.repositoryIterator = null;
                return;
            }
        }
        this.repositoryIterator = iterator.iterator();
    }

    private PagedSearchIterable<GHRepository> getNextIterable() {
        var gitHub = tokenProvider.nextClient();

        var startTime = currentTime;
        var endTime = currentTime.plusSeconds(secondDelta);
        if (endTime.isBefore(startTime)) {
            endTime = searchEnd;
        }

        var searchString = String.format("%sT%s..%sT%s", searchDate, timeFormatter.format(startTime), searchDate, timeFormatter.format(endTime));
        PagedSearchIterable<GHRepository> searchResult = gitHub.searchRepositories()
            .created(searchString)
            .list()
            .withPageSize(100);

        if(searchResult.getTotalCount() > githubSearchMax && secondDelta > 1) {
            secondDelta = secondDelta / 2;
            if (secondDelta < minSecondDelta) {
                secondDelta = minSecondDelta;
            }
            return null;
        }

        if(searchResult.getTotalCount() < githubSearchMax / 3) {
            secondDelta = secondDelta * 3;
        }

        log.info("Github data initialized for date: {} and time: {} with search query result size: {}", searchDatetime, timeFormatter.format(currentTime), searchResult.getTotalCount());

        currentTime = endTime;
        return searchResult;
    }
}
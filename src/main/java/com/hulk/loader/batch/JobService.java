package com.hulk.loader.batch;

import com.hulk.loader.exception.ApplicationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobService {
    private final JobLauncherService jobLauncherService;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public List<Pair<LocalDateTime, Boolean>> startSeveralJobs(LocalDate from, LocalDate to) {
        if (from.isAfter(to)) {
            throw new ApplicationException("'from' must be after 'to'");
        }

        var jobCount = ChronoUnit.DAYS.between(from, to);

        if (jobCount < 0 || jobCount > 30) {
            throw new ApplicationException("Days count must be between 0 and 30");
        }

        return Stream.iterate(from, date -> date.plusDays(1))
            .limit(jobCount)
            .flatMap(date -> IntStream.range(0, 24).mapToObj(hour -> date.atTime(hour, 0)))
            .map(date -> Pair.of(date, startJob(date)))
            .toList();
    }

    public boolean startJob(LocalDateTime date) {
        try {
            jobLauncherService.startGithubJob(dateTimeFormatter.format(date));
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        jobLauncherService.startGithubJob("2024-01-01");
//        jobLauncherService.startGithubJob("2024-01-02");
//    }

}

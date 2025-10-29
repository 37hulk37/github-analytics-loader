package com.hulk.loader.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class JobLauncherService {

    private final JobLauncher jobLauncher;
    private final Job githubJob;

    public void startGithubJob(String searchDate) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("searchDate", searchDate, true) // true = параметр идентификации
            .addLong("startAt", System.currentTimeMillis())
            .toJobParameters();

        JobExecution jobExecution = jobLauncher.run(githubJob, jobParameters);

        log.info("Job started with ID: {}, parameters: {}",
            jobExecution.getId(), jobParameters);
    }
}
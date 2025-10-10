package com.hulk.loader.batch;

import com.hulk.loader.RepositoryBasicDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {


    @Bean
    public Step githubStep(
        JobRepository jobRepository,
        PlatformTransactionManager transactionManager,
        GithubClientReader reader,
        MinioItemWriter writer) {
        return new StepBuilder("githubStep", jobRepository)
            .<RepositoryBasicDto, RepositoryBasicDto>chunk(100, transactionManager) // Обрабатываем по 2 записи за раз
            .reader(reader)
            .writer(writer)
            .build();
    }

    @Bean
    public Job githubJob(
        Step githubStep,
        JobRepository jobRepository
    ) {
        return new JobBuilder("githubJob", jobRepository)
            .start(githubStep)
            .build();
    }

    @Bean
    public JobLauncherService githubJobLauncher(JobLauncher jobLauncher, Job githubJob) {
        return new JobLauncherService(jobLauncher, githubJob);
    }

    //todo: add task executor for parallel computing
}

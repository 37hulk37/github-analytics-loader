package com.hulk.loader.configuration;

import com.hulk.loader.RepositoryBasicDto;
import com.hulk.loader.batch.GithubClientReader;
import com.hulk.loader.batch.GithubWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Value("${app.executor.max-pool-size}")
    private Integer maxPoolSize;

    @Bean
    public Step githubStep(
        JobRepository jobRepository,
        PlatformTransactionManager transactionManager,
        GithubClientReader reader,
        GithubWriter writer) {
        return new StepBuilder("githubStep", jobRepository)
            .<RepositoryBasicDto, RepositoryBasicDto>chunk(500, transactionManager)
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
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(maxPoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setThreadNamePrefix("job-exec-");
        executor.initialize();
        return executor;
    }

    @Bean
    public JobLauncher jobLauncher(
        @Qualifier("batchTaskExecutor") TaskExecutor taskExecutor,
        JobRepository jobRepository
    ) throws Exception {
        var jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setTaskExecutor(taskExecutor);
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}

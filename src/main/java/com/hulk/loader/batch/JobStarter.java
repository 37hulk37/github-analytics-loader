package com.hulk.loader.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobStarter implements InitializingBean {
    private final JobLauncherService jobLauncherService;

    @Override
    public void afterPropertiesSet() throws Exception {
        jobLauncherService.startGithubJob("2024-01-01");
        jobLauncherService.startGithubJob("2024-01-02");
    }

    //todo: change to create a queue of dates
}

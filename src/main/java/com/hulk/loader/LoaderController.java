package com.hulk.loader;

import com.hulk.loader.batch.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.util.Pair;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/loader")
public class LoaderController {
    private final JobService jobService;

    @PostMapping("/job/start")
    public List<Pair<LocalDateTime, Boolean>> startJob(@RequestBody StartJobRequest request) {
        return jobService.startSeveralJobs(request.from, request.to);
    }

    public record StartJobRequest(
        LocalDate from, LocalDate to
    ) {
    }
}

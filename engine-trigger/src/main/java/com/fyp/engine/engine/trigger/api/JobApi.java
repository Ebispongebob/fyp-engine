package com.fyp.engine.engine.trigger.api;

import com.fyp.engine.job.AdpRtRuleEngine;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.Executors;

@RequestMapping("/api/job")
@RestController
@CrossOrigin
public class JobApi {

    @GetMapping("/create")
    public void create(@RequestParam String referenceId, @RequestParam String topic) {
        Executors.newFixedThreadPool(1).submit(() -> AdpRtRuleEngine.trigger(referenceId, topic));
    }
}

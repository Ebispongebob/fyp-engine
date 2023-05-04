package com.fyp.engine.engine.trigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EngineTriggerApplication {
    private static Logger logger = LoggerFactory.getLogger(EngineTriggerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(EngineTriggerApplication.class, args);
        logger.info("application started----------------------");
    }

}

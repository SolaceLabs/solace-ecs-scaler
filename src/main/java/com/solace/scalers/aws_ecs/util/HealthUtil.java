package com.solace.scalers.aws_ecs.util;

import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Log4j2
public class HealthUtil {

    private static Path tempFilePath;

    public static void updateHealthStatus(boolean isRunning) {

        if (isRunning) {
            if(tempFilePath == null) {
                try {
                    tempFilePath = Files.createTempFile(Paths.get(".").resolve("healthz"),"healthy", null);
                    log.info("Health check file created at: {}", tempFilePath.normalize());
                    tempFilePath.toFile().deleteOnExit();
                } catch (IOException e) {
                    log.error("Health check file creation failed", e);
                }
            }
        } else {
            tempFilePath.toFile().deleteOnExit();
            log.info("Health check file deleted");
        }
    }
}

package io.nson.arrowcache.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

public abstract class ConcurrencyUtils {
    private ConcurrencyUtils() {}

    public static void scheduleAtFixedRate(Timer timer, Runnable runnable, Duration delay) {
        timer.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        runnable.run();
                    }
                }, delay.toMillis(),
                delay.toMillis()
        );
    }
}

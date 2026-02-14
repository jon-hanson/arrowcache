package io.nson.arrowcache.server.utils;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

public abstract class ConcurrencyUtils {
    private ConcurrencyUtils() {}

    public static TimerTask scheduleAtFixedRate(Timer timer, Runnable runnable, Duration delay) {
        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                runnable.run();
            }
        };

        timer.scheduleAtFixedRate(
                task,
                delay.toMillis(),
                delay.toMillis()
        );

        return task;
    }
}

package com.shf.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * description :
 * https://projectreactor.io/docs/core/release/reference/#metrics
 *
 * @author songhaifeng
 * @date 2020/7/7 9:37
 */
public class MetricsDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsDemo.class);

    @Test
    public void metricsTest() {
        Schedulers.enableMetrics();
        Flux.range(1, 5)
                .tag("source", "kafka")
                .name("events")
                .metrics()
                .doOnNext(event -> LOGGER.info("Received {}", event))
                .delayUntil(event -> {
                    LOGGER.info("delay");
                    return Mono.just(event * 2);
                })
                .retry()
                .subscribe(event -> LOGGER.info("Consumer {}", event));
    }

}

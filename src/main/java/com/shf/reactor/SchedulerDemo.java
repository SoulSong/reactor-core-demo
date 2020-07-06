package com.shf.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

/**
 * Schedulers.immediate()：直接在当前线程执行，与没有使用一样
 * Schedulers.single()：所有调用者使用一个全局线程
 * Schedulers.newSingle()：每个调用者独立占有一个线程
 * Schedulers.elastic()： 不建议被使用，建议使用Schedulers.boundedElastic()，该调度器会造成线程过多(无界)
 * Schedulers.boundedElastic()：按需创建线程并复用空闲线程，空闲线程默认60s后丢弃销毁；其默认最多可创建线程数=CPU核心数*10
 * 线程达到上限后，最多可提交10W个任务，即队列大小为10W。特别适用于IO密集型任务
 * Schedulers.parallel()：创建的线程数与CPU核心数一样多，适用于CPU密集型任务
 * Schedulers.fromExecutorService(ExecutorService)：自定义
 * <p>
 * publishOn：仅会影响其下游的方法直到另一个publishOn出现；其比较适合快速生产缓慢消费的场景，如CPU密集型任务；
 * subscribeOn：会影响所有的方法(调用链中如果有多个subscribeOn，只有最早的第一个生效)，从item的生产开始；比较适合缓慢生产快速消费的场景，特别适合IO阻塞型任务
 * 故publishOn的使用时机以及位置非常重要，而subscribeOn不那么重要
 *
 * @author songhaifeng
 * @date 2017/11/6
 */
public class SchedulerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerDemo.class);

    /**
     * 所有操作默认在主线程执行
     */
    @Test
    public void log() {
        Flux.create(sink -> {
            for (int i = 0; i < 5; i++) {
                sink.next(i);
            }
            sink.complete();
        })
                .log()
                .subscribe(data -> LOGGER.info("当前线程(" + Thread.currentThread().getName() + "),消费数据：" + data));

    }

    /**
     * 所有的操作均会在subscribeOn指定的线程中执行，subscribeOn会影响上游方法执行线程；
     * subscribeOn适用于缓慢发布，快速消费的场景，如阻塞IO；
     *
     * @throws InterruptedException e
     */
    @Test
    public void subscribeOn() throws InterruptedException {
        int size = 5;
        CountDownLatch countDownLatch = new CountDownLatch(size);
        Flux.create(sink -> {
            for (int i = 0; i < size; i++) {
                LOGGER.info("produce {} in thread({})", i, Thread.currentThread().getName());
                sink.next(i);
            }
            sink.complete();
        })
                .log()
                // Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
                .subscribeOn(Schedulers.newParallel("sub"))
                .subscribe(data -> {
                    sleepWhile();
                    LOGGER.info("current thread(" + Thread.currentThread().getName() + "),consumed：" + data);
                    countDownLatch.countDown();
                });
        LOGGER.info("current thread(" + Thread.currentThread().getName() + "),waiting.");
        countDownLatch.await();
    }

    /**
     * publishOn仅会影响下游执行方法所在线程；
     * publishOn通常用于快速发布、缓慢消费的场景；
     */
    @Test
    public void publishOn() throws InterruptedException {
        int size = 10;
        CountDownLatch countDownLatch = new CountDownLatch(size);
        Flux.create(sink -> {
            for (int i = 0; i < size; i++) {
                LOGGER.info("produce {} in thread({})", i, Thread.currentThread().getName());
                sink.next(i);
            }
            sink.onDispose(() -> LOGGER.info("complete send all messages"));
            sink.complete();
        })
                .log()
                .publishOn(Schedulers.newParallel("pub"), 10)
                .subscribe(data -> {
                            sleepWhile();
                            LOGGER.info("current thread(" + Thread.currentThread().getName() + "),consumed：" + data);
                            countDownLatch.countDown();
                        },
                        System.err::println,
                        () -> LOGGER.info("Done"),
                        // 每次消费8个item
                        s -> s.request(size));
        LOGGER.info("current thread(" + Thread.currentThread().getName() + "),waiting.");
        countDownLatch.await();
    }

    private void sleepWhile() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * 再次验证subscribeOn会影响上游所有方法执行线程；
     *
     * @throws InterruptedException
     */
    @Test
    public void subscribeOn2() throws InterruptedException {
        int size = 5;
        CountDownLatch countDownLatch = new CountDownLatch(size);
        Flux.<Integer>create(sink -> {
            for (int i = 0; i < size; i++) {
                sink.next(i);
            }
            sink.complete();
        })
                .log()
                .map(data -> {
                    LOGGER.info("before subscribeOn,current thread(" + Thread.currentThread().getName() + "),{} * 2 = {}", data, data * 2);
                    return data * 2;
                })
                .doFinally(type -> {
                    //根据不同的终止类型设定不同的状态
                    if (type == SignalType.CANCEL) {
                        LOGGER.info("before subscribeOn,current thread(" + Thread.currentThread().getName() + "),invoke CANCEL");
                    } else if (type == SignalType.ON_COMPLETE) {
                        LOGGER.info("before subscribeOn,current thread(" + Thread.currentThread().getName() + "),invoke ON_COMPLETE");
                    }
                })
                .subscribeOn(Schedulers.newParallel("sub"))
                .map(data -> {
                    LOGGER.info("after subscribeOn,current thread(" + Thread.currentThread().getName() + "),{} * 2 = {}", data, data * 2);
                    return data * 2;
                })
                .doFinally(type -> {
                    //根据不同的终止类型设定不同的状态
                    if (type == SignalType.CANCEL) {
                        LOGGER.info("after subscribeOn,current thread(" + Thread.currentThread().getName() + "),invoke CANCEL");
                    } else if (type == SignalType.ON_COMPLETE) {
                        LOGGER.info("after subscribeOn,current thread(" + Thread.currentThread().getName() + "),invoke ON_COMPLETE");
                    }
                })
                .subscribe(data -> {
                            LOGGER.info("current thread(" + Thread.currentThread().getName() + "),consumed：" + data);
                            countDownLatch.countDown();
                        },
                        System.err::println,
                        () -> LOGGER.info("Done")
                );
        LOGGER.info("current thread(" + Thread.currentThread().getName() + "),waiting.");
        countDownLatch.await();
        sleepWhile();
    }

    /**
     * prefetch决定了异步生产消费的item个数(即prefetch个item在主线程生产，scheduler线程池消费)，超过prefetch设置值的item项生产消费均在自定义的scheduler中执行。
     *
     * @throws InterruptedException e
     */
    @Test
    public void publishOn2() throws InterruptedException {
        int size = 5;
        CountDownLatch countDownLatch = new CountDownLatch(size);
        Flux.<Integer>create(sink -> {
            for (int i = 0; i < size; i++) {
                sink.next(i);
            }
            sink.complete();
        })
                .log()
                .map(data -> {
                    LOGGER.info("before publishOn,current thread(" + Thread.currentThread().getName() + "),{} * 2 = {}", data, data * 2);
                    return data * 2;
                })
                .doFinally(type -> {
                    //根据不同的终止类型设定不同的状态
                    if (type == SignalType.CANCEL) {
                        LOGGER.info("before publishOn,current thread(" + Thread.currentThread().getName() + "),invoke CANCEL");
                    } else if (type == SignalType.ON_COMPLETE) {
                        LOGGER.info("before publishOn,current thread(" + Thread.currentThread().getName() + "),invoke ON_COMPLETE");
                    }
                })
                //prefetch值决定了request多少item由主线程生产并由子线程消费，超出设置值的items均由scheduler调度器生产消费。
                .publishOn(Schedulers.newParallel("pub"), 2)
                .map(data -> {
                    LOGGER.info("after publishOn,current thread(" + Thread.currentThread().getName() + "),{} * 2 = {}", data, data * 2);
                    return data * 2;
                })
                .doFinally(type -> {
                    //根据不同的终止类型设定不同的状态
                    if (type == SignalType.CANCEL) {
                        LOGGER.info("after publishOn,current thread(" + Thread.currentThread().getName() + "),invoke CANCEL");
                    } else if (type == SignalType.ON_COMPLETE) {
                        LOGGER.info("after publishOn,current thread(" + Thread.currentThread().getName() + "),invoke ON_COMPLETE");
                    }
                })
                .subscribe(data -> {
                            sleepWhile();
                            LOGGER.info("current thread(" + Thread.currentThread().getName() + "),consumed：" + data);
                            countDownLatch.countDown();
                        },
                        System.err::println,
                        () -> LOGGER.info("Done")
                );
        LOGGER.info("current thread(" + Thread.currentThread().getName() + "),waiting.");
        countDownLatch.await();
        sleepWhile();
    }

    /**
     * 当subscribeOn和publishOn被混合使用时，publishOn会影响其下游所有方法，而subscribeOn会从item的生产开始影响
     */
    @Test
    public void mixSchedulerTest() {
        AtomicBoolean state = new AtomicBoolean();
        Disposable disposable = Flux.create(sink -> {
            // 将在subscribeOn指定的线程池中执行
            LOGGER.info("produce item : {}", Thread.currentThread().getName());
            sink.next(Thread.currentThread().getName());
            sink.complete();
        })
                .doOnNext(item -> {
                    LOGGER.info("consume data on next: {}", item);
                })
                .publishOn(Schedulers.single())
                .map(x -> {
                    LOGGER.info("Append current threadName : {}", Thread.currentThread().getName());
                    return String.format("[%s] %s", Thread.currentThread().getName(), x);
                })
                .doOnNext(item -> {
                    LOGGER.info("consume data on next: {}", item);
                })
                .publishOn(Schedulers.elastic())
                .map(x -> {
                    LOGGER.info("Append current threadName : {}", Thread.currentThread().getName());
                    return String.format("[%s] %s", Thread.currentThread().getName(), x);
                })
                .doOnNext(item -> {
                    LOGGER.info("consume data on next: {}", item);
                })
                .subscribeOn(Schedulers.parallel())
                .subscribe(data -> {
                    // 最终消费在最后一个publishOn指定的elastic调度器中
                    LOGGER.info("consume data : {}", data);
                    state.set(true);
                });
        while (!disposable.isDisposed()) {
            if (state.get()) {
                disposable.dispose();
            }
        }
    }

}

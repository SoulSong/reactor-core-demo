package com.shf.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.function.TupleUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.between;

/**
 * 参考：http://www.infoq.com/cn/articles/reactor-by-example
 *
 * @author songhaifeng
 * @date 2017/10/10
 */
public class ReactorSnippets {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReactorSnippets.class);
    private static List<String> words = Arrays.asList(
            "the",
            "quick",
            "brown",
            "fox",
            "jumped",
            "over",
            "the",
            "lazy",
            "dog"
    );

    @Test
    public void findingMissingLetter() {
        System.out.println("--------------方式一-------------");
        Flux.fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort()
                .zipWith(Flux.range(0, Integer.MAX_VALUE),
                        (string, count) -> String.format("%2d. %s", count, string))
                .subscribe(System.out::println);

        System.out.println("--------------方式二-------------");
        Flux.fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort()
                // 给每个元素添加index，构建成二元组
                .index()
                .subscribe(tuple -> System.out.println(tuple.getT1() + "." + tuple.getT2()));
    }

    /**
     * 使用concat/concatWith和一个Mono来手动往字母Flux里添加“s”
     */
    @Test
    public void restoringMissingLetter() {
        System.out.println("*************方式1*************");
        Mono<String> missing = Mono.just("s");
        Flux<String> allLetters = Flux
                .fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .concatWith(missing)
                .distinct()
                .sort()
                .zipWith(Flux.range(1, Integer.MAX_VALUE),
                        (string, count) -> String.format("%2d. %s", count, string));

        allLetters.subscribe(System.out::println);

        System.out.println("*************方式2*************");

        Mono<String> missing2 = Mono.just("str");
        Flux<String> allLetters2 = Flux
                .fromIterable(words)
                .concatWith(missing2)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort()
                .zipWith(Flux.range(1, Integer.MAX_VALUE),
                        (string, count) -> String.format("%2d. %s", count, string));

        allLetters2.subscribe(System.out::println);
    }

    /**
     * 在主线程里对事件源进行订阅无法完成更加复杂的异步操作，
     * 主要是因为在订阅完成之后，控制权会马上返回到主线程，并退出整个程序
     */
    @Test
    public void shortCircuit() {
        Flux<String> helloPauseWorld =
                Mono.just("Hello")
                        .concatWith(Mono.just("world")
                                .delaySubscription(Duration.ofMillis(500)))
                        .doOnSubscribe(subscription -> System.out.println("new subscription"));

        helloPauseWorld.subscribe(System.out::println);
    }

    /**
     * 使用一些操作转换到非响应式模式。toItetable和toStream会生成阻塞实例
     */
    @Test
    public void blocks() {
        Flux<String> helloPauseWorld =
                Mono.just("Hello")
                        .concatWith(Mono.just("world")
                                .delaySubscription(Duration.ofMillis(450)));

        System.out.println("*************toStream方式*************");
        helloPauseWorld.toStream()
                .forEach(System.out::println);

        System.out.println("*************toIterable方式*************");
        helloPauseWorld.toIterable()
                .forEach(System.out::println);
    }

    /**
     * RxJava的amb操作在Reactor里被重命名为firstEmitting（正如它的名字所表达的：选择第一个Flux来触发）
     * 这个单元测试会打印出句子的所有部分，它们之间有400毫秒的时间间隔。
     */
    @Test
    public void firstEmitting() {
        Mono<String> a = Mono.just("oops I'm late")
                .delaySubscription(Duration.ofMillis(450));
        Flux<String> b = Flux.just("let's get", "the party", "started")
                //每个实例处理间隔
                .delayElements(Duration.ofMillis(400));

        Flux.first(a, b)
                .toIterable()
                .forEach(System.out::println);
    }

    @Test
    public void switchOnFirst() {
        Flux.range(1, 5)
                .switchOnFirst((signal, integerFlux) -> {
                    System.out.println("first item is " + signal.get());
                    return integerFlux.map(i -> i * 2);
                })
                .subscribe(System.out::println);
    }

    /**
     * 如果3种方式，type3最高效，type1和type2整体一致
     * 场景，通过延迟模拟网络请求
     *
     * @throws InterruptedException e
     */
    @Test
    public void zip() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(3);
        Instant start = Instant.now();

        Mono.zip(getYear(), getName())
                .flatMap(TupleUtils.function((year, name) -> updateByYearAndName(findByYear(Mono.just(year)), Mono.just(name))))
                .subscribe(result -> {
                    LOGGER.info("type1----->result:{}, spend:{}", result, between(Instant.now(), start));
                    countDownLatch.countDown();
                });

        Mono.zip(getYear(), getName())
                .flatMap(TupleUtils.function((year, name) -> {
                    return findByYear(Mono.just(year))
                            .flatMap(y -> updateByYearAndName(Mono.just(y), Mono.just(name)));
                }))
                .subscribe(result -> {
                    LOGGER.info("type2----->result:{}, spend:{}", result, between(Instant.now(), start));
                    countDownLatch.countDown();
                });

        // 最高效，其中getYear优先返回后即可立即执行findByYear方法，无需等待getName返回，故减少了1S的延迟
        Mono.zip(findByYear(getYear()), getName())
                .flatMap(TupleUtils.function((year, name) -> updateByYearAndName(Mono.just(year), Mono.just(name))))
                .subscribe(result -> {
                    LOGGER.info("type3----->result:{}, spend:{}", result, between(Instant.now(), start));
                    countDownLatch.countDown();
                });

        countDownLatch.await();
    }

    private Mono<Integer> getYear() {
        return Mono.delay(Duration.ofSeconds(1)).thenReturn(2021).doOnSubscribe(subscription -> {
            LOGGER.info("Subscribe getYear.");
        });
    }

    private Mono<String> getName() {
        return Mono.delay(Duration.ofSeconds(2)).thenReturn("song").doOnSubscribe(subscription -> {
            LOGGER.info("Subscribe getName.");
        });
    }

    private Mono<Integer> findByYear(Mono<Integer> year) {
        return year.delaySubscription(Duration.ofSeconds(1))
                .flatMap(y -> {
                    if (y > 2020) {
                        return Mono.just(y + 1000);
                    }
                    return Mono.just(y - 1000);
                });
    }

    private Mono<String> updateByYearAndName(Mono<Integer> year, Mono<String> name) {
        return year.flatMap(y -> name.flatMap(n -> Mono.just("year:[" + y + "] and name:[" + n + "]")));
    }

    /**
     * limitRequest约束了每个消费者最多消费的数量,而limitRate约束了每次request最多个数
     */
    @Test
    public void limitRequestTest() {
        LOGGER.info("Consumer with no limit");
        Flux.range(1, 5)
                .take(3)
                .subscribe(event -> LOGGER.info("{}", event));

        LOGGER.info("Consumer with limit");
        Flux.range(1, 5)
                .limitRequest(2)
                .take(3)
                .subscribe(event -> LOGGER.info("{}", event));
    }

    /**
     * 实现批处理
     */
    @Test
    public void bufferTest() {
        Flux.range(1, 5)
                .buffer(2)
                .doOnNext(list -> LOGGER.info("items : {}", list))
                .subscribe(event -> LOGGER.info("{}", event));
    }

    /**
     * 同样可以实现批处理，但不同于buffer：
     * buffer先阻塞收集，直到满足条件后开始被消费，完全消费后进入第一个阻塞收集期，如此反复循环；
     * window先消费，直到满足条件进入下一个窗口；
     */
    @Test
    public void windowTest() {
        LOGGER.info("Consumer without collect");
        Flux.range(1, 5)
                .window(2)
                .subscribe(events -> {
                    LOGGER.info("consumer current window:");
                    events.subscribe(event -> LOGGER.info("{}", event));
                });

        LOGGER.info("Consumer with collect");
        Flux.range(1, 5)
                .window(2)
                .subscribe(window -> {
                    LOGGER.info("consumer current window:");
                    window.collectList().subscribe(event -> LOGGER.info("{}", event));
                });
    }
}
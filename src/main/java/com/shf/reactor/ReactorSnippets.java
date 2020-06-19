package com.shf.reactor;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 参考：http://www.infoq.com/cn/articles/reactor-by-example
 *
 * @author songhaifeng
 * @date 2017/10/10
 */
public class ReactorSnippets {
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
}
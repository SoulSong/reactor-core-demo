package com.shf.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * 错误处理示例
 *
 * @author songhaifeng
 * @date 2017/11/6
 */
public class ErrorDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorDemo.class);

    /**
     * Mono可以通过doOnSuccessOrError方法实现对正常和异常的处理，特别适合通过create动态创建源的场景
     */
    @Test
    public void doOnSuccessOrError() {
        int flag = 0;
        Mono.create(monoSink -> {
            if (flag == 0) {
                monoSink.success("hello");
            } else {
                throw new IllegalStateException("error");
            }
        }).doOnSuccessOrError((data, e) -> {
            if (e == null) {
                System.out.println(data);
            } else {
                System.out.println(e.getMessage());
            }
        }).subscribe();
    }

    /**
     * 通过subscribe方法来添加相应的订阅逻辑，实现对正常消息和异常消息的处理
     */
    @Test
    public void errorSimple() {
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException()))
                .subscribe(System.out::println, System.err::println);
    }

    /**
     * 大部分的操作均提供了doOn对应的方法，在subscribe方法的第二个参数errorConsumer，其对应了doOnError方法，
     * 同样能够对异常进行定制化处理
     */
    @Test
    public void doOnError() {
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("IllegalStateException")))
                .concatWith(Mono.just(3))
                .doOnError(e -> System.out.println("Error1:" + e.getMessage()))
                .doOnError(e -> System.out.println("Error2:" + e.getMessage()))
                .subscribe(System.out::println, e -> System.out.println("Error3:" + e.getMessage()));
    }

    /**
     * onErrorMap()方法能够将A异常转换为B异常，然后进行后续处理
     */
    @Test
    public void onErrorMap() throws InterruptedException {
        System.out.println("*****************方式一*******************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("IllegalStateException")))
                .onErrorMap(e -> new IllegalArgumentException("IllegalArgumentException"))
                .subscribe(System.out::println, System.err::println);
        Thread.sleep(500);

        System.out.println("****************方式二********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("IllegalStateException")))
                .onErrorMap(IllegalStateException.class, e -> new IllegalArgumentException("IllegalArgumentException"))
                .subscribe(System.out::println, System.err::println);
        Thread.sleep(500);

        System.out.println("****************方式三********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("IllegalStateException")))
                .onErrorMap(e -> e instanceof IllegalStateException, e -> new IllegalArgumentException("IllegalArgumentException"))
                .subscribe(System.out::println, System.err::println);
    }

    /**
     * 静态回退值：通过onErrorReturn()方法返回一个默认值
     * 场景描述：出现异常，则返回默认值-1
     */
    @Test
    public void onErrorReturn() {
        System.out.println("*****************方式一*******************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException()))
                .onErrorReturn(-1)
                .subscribe(System.out::println);

        System.out.println("****************方式二********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException()))
                .onErrorReturn(IllegalStateException.class, -1)
                .subscribe(System.out::println);

        System.out.println("****************方式三********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException()))
                .onErrorReturn(e -> e instanceof IllegalArgumentException, -1)
                .onErrorReturn(e -> e instanceof IllegalStateException, -2)
                .subscribe(System.out::println);
    }

    /**
     * 回退方法：通过onErrorResume()方法获取对应的异常，并使用另外的Publisher实例来产生元素
     * 其借助指定异常类型，亦或通过predicate表达式对指定类型的错误进行对应处理的重载实现
     */
    @Test
    public void onErrorResume() {
        System.out.println("*****************方式一*******************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException()))
                .onErrorResume(e -> {
                    if (e instanceof IllegalStateException) {
                        return Mono.just(0);
                    } else if (e instanceof IllegalArgumentException) {
                        return Mono.just(-1);
                    }
                    return Mono.empty();
                })
                .subscribe(System.out::println);

        System.out.println("****************方式二********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalArgumentException()))
                .onErrorResume(IllegalAccessException.class, e -> Mono.just(-1))
                .onErrorResume(IllegalArgumentException.class, e -> Mono.just(-2))
                .subscribe(System.out::println);

        System.out.println("****************方式三********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalArgumentException()))
                .onErrorResume(e -> e instanceof IllegalArgumentException, e -> Mono.just(-1))
                .onErrorResume(e -> e instanceof IllegalAccessException, e -> Mono.just(-2))
                .subscribe(System.out::println);

        System.out.println("****************异常转换********************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalArgumentException("IllegalArgumentException")))
                .onErrorResume(e -> e instanceof IllegalArgumentException,
                        e -> Mono.error(new IllegalAccessException("IllegalAccessException")))
                .subscribe(System.out::println, System.err::println);
    }

    /**
     * 当出现错误时，还可以通过 retry 操作符来进行重试
     * 重试的动作是通过重新订阅序列来实现的,指定重试次数即可
     */
    @Test
    public void retry() throws InterruptedException {
        System.out.println("**************优先重试，重试仍然失败则通过使用其他源****************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("exception for test")))
                .retry(1)
                .onErrorResume(e -> e instanceof IllegalStateException, e -> Flux.just(-1, -2))
                .subscribe(System.out::println);
        Thread.sleep(500);

        System.out.println("************优先使用其他源，然后重试，显然重试是不会生效的*************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("exception for test")))
                .onErrorResume(e -> e instanceof IllegalStateException, e -> Flux.just(-1, -2))
                .retry(1)
                .subscribe(System.out::println);
        Thread.sleep(500);

        System.out.println("**************优先重试，重试仍然失败则通过使用其他源****************");
        Flux.just(1, 2)
                .concatWith(Mono.error(new IllegalStateException("exception for test")))
                .retry(1)
                .subscribe(System.out::println, System.err::println);
    }

    /**
     * retryWhen较retry，即使不进行任何的Error处理，其也能complete successfully；
     * retry会在最后一次重试至Error处终止
     */
    @Test
    public void retryWhen() {
        Flux.just(1, 2, 3)
                .concatWith(Mono.error(new IllegalStateException("IllegalStateException")))
                .concatWith(Mono.just(4))
                //如果放置在retry后面则无法看到异常信息
                .doOnError(e -> System.out.println("处理异常：" + e.getMessage()))
                .retryWhen(companion -> companion.take(1))
                .subscribe(
                        data -> System.out.println("消费数据：" + data),
                        System.err::println,
                        () -> System.out.println("Done"));
    }

    /**
     * doFinally类似于try-catch-finally，无论是complete、error还是cancel，只要是终止其均能够被执行，
     * 且可以根据SignalType终止类型进行定制化业务定义
     */
    @Test
    public void doFinally() {
        LongAdder state = new LongAdder();
//        Long consumerSize= 3L;
        Long consumerSize = 8L;
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        Flux.fromIterable(list)
//                .concatWith(Mono.error(new IllegalStateException("exception for test")))
                .doFinally(type -> {
                    //根据不同的终止类型设定不同的状态
                    if (type == SignalType.CANCEL) {
                        state.add(consumerSize);
                    } else if (type == SignalType.ON_COMPLETE) {
                        state.add(list.size());
                    } else if (type == SignalType.ON_ERROR) {
                        state.add(-1);
                    }
                })
                //take设定生产发布一定数目消息后即cancel
                .take(consumerSize)
                .subscribe(System.out::println, System.err::println);

        System.out.println("实际消费" + state.longValue() + "个消息");
    }

    /**
     * 此示例主要描述，在生产发布过程中，只要出现error，则立即退出(无论是否进行了Error处理)
     * 场景描述：250毫秒生产发布一个自增数字，当发布3个以后抛出异常，停止生产
     *
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void terminate() throws InterruptedException {
        Flux.<String>interval(Duration.ofMillis(250))
                .map(input -> {
                    if (input < 3) {
                        return "tick " + input;
                    }
                    throw new RuntimeException("error");
                })
                .onErrorReturn("handle error")
                .subscribe(System.out::println);
        Thread.sleep(3000);
    }

}

package com.shf.reactor;

import org.junit.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * 官方指南：https://projectreactor.io/docs/core/release/reference/#producing
 * https://www.ibm.com/developerworks/cn/java/j-cn-with-reactor-response-encode/index.html?lnk=hmhm
 * Created by songhaifeng on 2017/10/11.
 */
public class FluxDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(FluxDemo.class);

    //****************************静态创建源******************************

    /**
     * 多方式创建Flux实例
     */
    @Test
    public void staticCreate() {
        //可以指定序列中包含的全部元素
        Flux.just("foo", "bar", "foobar").subscribe(System.out::println);

        //可以从一个数组、Iterable 对象或 Stream 对象中创建 Flux 对象
        Flux.fromIterable(Arrays.asList("foo", "bar", "foobar")).subscribe(System.out::println);
        Flux.fromArray(new String[]{"foo", "bar", "foobar"}).subscribe(System.out::println);
        Flux.fromStream(Stream.of("foo", "bar", "foobar")).subscribe(System.out::println);

        //创建一个不包含任何元素，只发布结束消息的序列。
        Flux.<String>empty().subscribe(System.out::println);

        //创建一个只包含错误消息的序列
        Flux.error(new RuntimeException("Error")).subscribe(System.out::println, System.err::println);

        //创建一个不包含任何消息通知的序列
        Flux.never().subscribe(System.out::println);

        //The first parameter is the start of the range,
        //while the second parameter is the number of items to produce.
        //创建包含从 start 起始的 count 个数量的 Integer 对象的序列
        Flux.<Integer>range(5, 3).subscribe(System.out::println);

        //创建一个包含了从 0 开始递增的 Long 对象的序列。
        //其中包含的元素按照指定的间隔来发布。除了间隔时间之外
        System.out.println("1s生产一个0开始的Long序列(intervalMillis与interval用法一致，其默认采用毫秒作为单位):");
        AtomicLong state = new AtomicLong(0);
        Disposable disposable = Flux.<Long>interval(Duration.of(1, ChronoUnit.SECONDS)).subscribe(data -> {
            System.out.println(data);
            state.incrementAndGet();
        });
        while (!disposable.isDisposed()) {
            if (10 == state.get()) {
                disposable.dispose();
            }
        }

        //还可以指定起始元素发布之前的延迟时间。
        System.out.println();
        System.out.println("延迟5S后每1s生产一个0开始的Long序列:");
        long start = System.currentTimeMillis();
        AtomicLong delayState = new AtomicLong(0);
        Disposable delayDisposable = Flux.<Long>interval(Duration.of(5, ChronoUnit.SECONDS), Duration.of(1, ChronoUnit.SECONDS)).subscribe(data -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",生产到消费耗时：" + (System.currentTimeMillis() - start) + ",生产的数据为：" + data);
            delayState.incrementAndGet();
        });
        while (!delayDisposable.isDisposed()) {
            if (10 == delayState.get()) {
                delayDisposable.dispose();
            }
        }
        System.out.println("主线程" + Thread.currentThread().getId() + "耗时：" + (System.currentTimeMillis() - start));


    }

    //******************************动态创建源**********************************8

    /**
     * generate()方法通过同步和逐一的方式来产生 Flux 序列。
     * 序列的产生是通过调用所提供的 SynchronousSink 对象的 next()，complete()和 error(Throwable)方法来完成的。
     * 逐一生成的含义是在具体的生成逻辑中，next()方法只能最多被调用一次。在有些情况下，序列的生成可能是有状态的，
     * 需要用到某些状态对象。此时可以使用 generate()方法的另外一种形式 generate(Callable<S> stateSupplier, BiFunction<S,SynchronousSink<T>,S> generator)，
     * 其中 stateSupplier 用来提供初始的状态对象。在进行序列生成时，状态对象会作为 generator 使用的第一个参数传入，
     * 可以在对应的逻辑中对该状态对象进行修改以供下一次生成时使用。
     */
    @Test
    public void baseGenerate() {
        Flux.generate(sink -> {
            sink.next("Hello");
            sink.complete();
        }).subscribe(System.out::println);

        System.out.println("-----------------------");

        Flux.generate(
                //初始state值
                () -> 0,
                (state, sink) -> {
                    //生产待消费数据
                    sink.next("3 x " + state + " = " + 3 * state);
                    //控制生产终止
                    if (state == 10) {
                        sink.complete();
                    }
                    //修改state状态，返回用于下次生产判断是否终止
                    return state + 1;
                }).subscribe(System.out::println);
    }

    /**
     * 通过可变引用变量生产数据
     */
    @Test
    public void mutableGenerate() {
        Flux<String> flux = Flux.generate(
                //此处作为计数器
                AtomicLong::new,
                (state, sink) -> {
                    long i = state.getAndIncrement();
                    sink.next("3 x " + i + " = " + 3 * i);
                    if (i == 10) {
                        sink.complete();
                    }
                    return state;
                });
        flux.subscribe(System.out::println);

        System.out.println("-----------------------");

        final Random random = new Random();
        Flux.generate(ArrayList::new, (list, sink) -> {
            int value = random.nextInt(100);
            list.add(value);
            //后续真实处理数据源
            sink.next(value + "--test");
            if (list.size() == 10) {
                sink.complete();
            }
            return list;
        }).subscribe(System.out::println);
    }

    /**
     * 加入Consumer lambda，可以通过此lambda对原始资源进行消费后的处理，如数据库连接释放等
     */
    @Test
    public void mutableGenerate2() {
        Flux<String> flux = Flux.generate(
                //0开始
                AtomicLong::new,
                (state, sink) -> {
                    long i = state.getAndIncrement();
                    sink.next("3 x " + i + " = " + 3 * i);
                    if (i == 10) {
                        sink.complete();//此时state为11
                    }
                    return state;
                }, (state) -> System.out.println("state: " + state));
        flux.subscribe(System.out::println);
    }

    /**
     * create()方法与 generate()方法的不同之处在于所使用的是 FluxSink 对象。
     * FluxSink 支持同步和异步的消息产生，并且可以在一次调用中产生多个元素。
     */
    @Test
    public void fluxCreate() {
        //在一次调用中就产生了全部的 10 个元素。
        Flux.create(sink -> {
            for (int i = 0; i < 10; i++) {
                sink.next(i);
            }
            sink.complete();
        }).log().subscribe(data -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",消费数据为：" + data);
        });
    }

    /**
     * 场景描述：消费一个字符串集合"start", "restart", "end"，生产发出"restart"后终止生产；
     * FluxSink使用了回压，所以你必须提供一个OverflowStrategy来显式地处理回压
     * FluxSink.OverflowStrategy.BUFFER：默认方式，缓冲所有信号（消息），即时消费跟生产，故其可能造成OutOfMemoryError;
     * FluxSink.OverflowStrategy.IGNORE：完全忽视消费者的背压要求，如果生产和消费的速率不能很好的同步，可能会导致IllegalStateException异常;
     * FluxSink.OverflowStrategy.ERROR：当消费者跟不上生产时，发出一个IllegalStateException异常;
     * FluxSink.OverflowStrategy.DROP：如果消费者来不及消费生产的消息，则直接丢弃onNext()生产（输入）的信号（消息）；
     * FluxSink.OverflowStrategy.LATEST：消费者将只消费来自生产的最新信号（消息）。
     */
    @Test
    public void fluxCreate2() {
        EventProcessor<String> myEventProcessor = new EventProcessor<>(Arrays.asList("start", "restart", "end"));
        Flux.create(sink -> {
                    myEventProcessor.register(
                            new EventListener<String>() {
                                @Override
                                public void onDataChunk(List<String> chunk) {
                                    for (String s : chunk) {
                                        sink.next(s);
                                        if ("restart".equalsIgnoreCase(s)) {
                                            processComplete();
                                        }
                                    }
                                }

                                @Override
                                public void processComplete() {
                                    sink.complete();
                                }
                            });
                }
                //BUFFER为默认方式
                , FluxSink.OverflowStrategy.BUFFER)
                .subscribe(System.out::println, System.err::println, () -> System.out.print("Done"));
    }

    interface EventListener<T> {
        void onDataChunk(List<T> chunk);

        void processComplete();
    }

    class EventProcessor<T> {
        List<T> chuck;

        EventProcessor(List<T> chuck) {
            this.chuck = chuck;
        }

        void register(EventListener<T> eventListener) {
            eventListener.onDataChunk(chuck);
            eventListener.processComplete();
        }

        void register(SingleThreadEventListener<T> eventListener) {
            try {
                eventListener.onDataChunk(chuck);
                eventListener.processComplete();
            } catch (Exception e) {
                eventListener.processError(e);
            }
        }
    }

    /**
     * create的一个变体是push，它适合于处理单个生产者的事件。
     * 类似于create，push也可以是异步的，并且可以使用任何溢出策略来管理背压。
     * 只有一个生产线程可能会一次调用next，complete或error，
     * 即next，complete与error事件在同一个线程中执行且仅有一个线程。
     */
    @Test
    public void fluxPush() {
        EventProcessor<String> myEventProcessor = new EventProcessor<>(Arrays.asList("start", "end", "error", "restart"));
        Flux.push(sink -> {
            myEventProcessor.register(
                    new SingleThreadEventListener<String>() {

                        @Override
                        public void onDataChunk(List<String> chuck) {
                            for (String s : chuck) {
                                //Events are pushed to the sink using next from a single listener thread
                                if ("error".equalsIgnoreCase(s)) {
                                    throw new IllegalStateException("sorry,something wrong.");
                                }
                                sink.next(s);
                            }
                        }

                        @Override
                        public void processComplete() {
                            //complete event generated from the same listener thread.
                            sink.complete();
                        }

                        @Override
                        public void processError(Throwable e) {
                            //error event also generated from the same listener thread.
                            sink.error(e);
                        }
                    });
        }).subscribe(System.out::println
                , error -> System.out.println("Error: " + error.getMessage())
                , () -> {
                    System.out.println("Done");
                });
    }

    interface SingleThreadEventListener<T> {
        void onDataChunk(List<T> chunk);

        void processComplete();

        void processError(Throwable e);
    }

    /**
     * Hybrid push/pull model
     */
    @Test
    public void fluxSink() {
        MessageProcessor myMessageProcessor = new MessageProcessor();
        Flux.create(sink -> {
            //pull模式
            sink.onRequest(n -> {
                System.out.println("pull 消费" + (n - 3) + "个消息");
                for (int i = 0; i < n - 3; i++) {
                    sink.next("poll:" + i);
                }
            }).onDispose(() -> System.out.println("invoke onDispose"))
                    .onCancel(() -> System.out.println("invoke onCancel"));
            //push模式
            myMessageProcessor.register(
                    messages -> {
                        System.out.println("push 消费" + messages.size() + "个消息");
                        for (String s : messages) {
                            sink.next(s);
                        }
                    });

            sink.complete();
        }).subscribe(System.out::println,
                System.err::println,
                () -> System.out.println("Done"),
                s -> s.request(10));
    }

    @FunctionalInterface
    interface MessageListener<T> {
        void onMessage(List<T> chuck);
    }

    class MessageProcessor {
        List<String> messages = Arrays.asList("start", "end", "restart");

        void register(MessageListener<String> messageListener) {
            messageListener.onMessage(messages);
        }
    }

    /**
     * 需要结合Flux或Mono使用，与generate比较相近，为同步并一个一个依次生产，
     * 通过handle结合sink可实现类似map、filter的应用场景
     */
    @Test
    public void handleTest() {
        AtomicLong state = new AtomicLong(0);
        Flux.just(-1, 30, 13, 9, 20)
                .handle((data, sink) -> {
                    //类似map操作，进行数据转换
                    String letter = alphabet(data);
                    //生产两个成功即停止生产
                    if (state.get() == 1) {
                        sink.complete();
                    }
                    //类似filter操作
                    if (letter != null) {
                        sink.next(letter);
                        state.incrementAndGet();
                    }
                }).subscribe(System.out::println,
                System.err::println,
                () -> System.out.println("Done"));
    }

    private String alphabet(int letterNumber) {
        if (letterNumber < 1 || letterNumber > 26) {
            return null;
        }
        int letterIndexAscii = 'A' + letterNumber - 1;
        return "" + (char) letterIndexAscii;
    }

    /*************************************消费处理*******************************/


    /**
     * 错误处理
     */
    @Test
    public void errorTest() {
        //模拟处理4时触发异常
        Flux<Integer> ints = Flux.range(1, 4)
                .map(i -> {
                    if (i <= 3) {
                        return i;
                    }
                    throw new RuntimeException("Got to 4");
                });
        ints.subscribe(System.out::println,
                error -> System.err.println("Error: " + error));
    }

    /**
     * 完成事件处理
     */
    @Test
    public void completeTest() {
        Flux<Integer> ints = Flux.range(1, 4);
        ints.subscribe(System.out::println,
                error -> System.err.println("Error " + error),
                () -> {
                    System.out.println("Done");
                });
    }

    /**
     * 通过subscriptionConsumer指定消费的数量
     */
    @Test
    public void subscriberTest() {
        Flux.just("foo", "bar", "foobar")
                .doOnCancel(() -> {
                    System.out.println("invoke cancel event.");
                }).subscribe(
                data -> System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",消费数据为：" + data),
                error -> System.err.println("Error " + error),
                () -> System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",Done"),
                //此处request中表示消费的数量，由于总的消费数量小于总量就发出了取消订阅指令，故不会完全消费以及触发complete事件
                s -> {
                    s.request(2);
                    s.cancel();
                });
    }

    /**
     * 自定义订阅者
     */
    @Test
    public void customSubscriberTest() {
        Flux.range(1, 8).subscribe(new SampleSubscriber<>());
    }

    class SampleSubscriber<T> extends BaseSubscriber<T> {

        @Override
        public void hookOnSubscribe(Subscription subscription) {
            System.out.println("Subscribed");
            //订阅后首次消费的数目
            request(2);
        }

        @Override
        public void hookOnNext(T value) {
            System.out.println(value);
            //每次消费数目(不包括订阅后的首次消费)
            request(1);
        }

        @Override
        protected void hookOnComplete() {
            System.out.println("Done");
        }
    }

    /**
     * 通过using实现资源的释放
     */
    @Test
    public void fluxUsing() {
        //模拟为资源
        AtomicBoolean isDisposed = new AtomicBoolean();
        System.out.println("Disposed状态：" + isDisposed);
        Disposable disposableInstance = new Disposable() {
            //完成后进行资源释放
            @Override
            public void dispose() {
                isDisposed.set(true);
            }

            @Override
            public String toString() {
                return "DISPOSABLE";
            }
        };
        Flux.using(
                () -> disposableInstance,
                disposable -> Flux.just(disposable.toString()),
                Disposable::dispose
        ).subscribe(System.out::println, System.err::println);

        System.out.println("Disposed状态：" + isDisposed);
    }
}


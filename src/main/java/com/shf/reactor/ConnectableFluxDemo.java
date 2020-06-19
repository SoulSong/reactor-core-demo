package com.shf.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

/**
 * 主要实现ConnectableFlux动态源，多消费场景验证
 *
 * @author songhaifeng
 * @date 2018/1/5
 */
public class ConnectableFluxDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectableFluxDemo.class);

    /**
     * 通过ConnectableFlux实现动态源，复用消费。生产者仅仅被订阅一次，在订阅后方生成数据。
     */
    @Test
    public void dynamicSubscribePush() {
        Flux<String> bridge = Flux.defer(() -> Flux.fromArray(new String[]{"start", "restart", "end"}))
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"));
        ConnectableFlux<String> hot = bridge.publish();
        hot.subscribe(data -> LOGGER.info("first subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        hot.subscribe(data -> LOGGER.info("second subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        hot.connect();
    }

    /**
     * push模式：一旦订阅完成，就可以自动完成相同的工作,通过autoConnect设定同时支持固定数量N的订阅者：
     * 1、若订阅者小于N个，则无法触发订阅；
     * 2、若超过订阅者N个，则超出的订阅者会被忽略；
     */
    @Test
    public void autoConnectSubscribePush() throws InterruptedException {
        Flux<String> bridge = Flux.fromArray(new String[]{"start", "restart", "end"})
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"));
        Flux<String> autoCo = bridge.publish().autoConnect(2);

        fiveSubscriber(autoCo);
    }

    /**
     * push模式：refCount设置最小订阅数量，如果不满足则不进行订阅消费处理，
     * 比如5个订阅者，设置refCount参数为2，则会执行2组，每2个一组(且同时根据订阅顺序依次消费)，最后一个订阅不会执行消费
     */
    @Test
    public void refCountSubscribePush() throws InterruptedException {
        Flux<String> bridge = Flux.fromArray(new String[]{"start", "restart", "end"})
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"));
        Flux<String> autoCo = bridge.publish().refCount(2);

        fiveSubscriber(autoCo);
    }

    /**
     * replay模式：设置最小订阅数量，第一次订阅需要满足设定的最小订阅数量，否则无法被消费；
     * 满足最小订阅数量后即可同时被连接消费，如果还有后续的订阅者，则一个一个顺序执行，不受限于refCount值
     */
    @Test
    public void refCountSubscribeReply() throws InterruptedException {
        Flux<String> bridge = Flux.fromArray(new String[]{"start", "restart", "end"})
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"));
        Flux<String> autoCo = bridge.replay().refCount(2);

        fiveSubscriber(autoCo);
    }

    private void fiveSubscriber(Flux<String> autoCo) throws InterruptedException {
        LOGGER.info("first subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("first subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("second subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("second subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("third subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("third subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("forth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("forth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("fifth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("fifth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        LOGGER.info("exit");
    }

    /**
     * 如果希望所有的订阅者能够消费一致的消息信息，则通过publish能够实现.
     * refCount约定了可消费的订阅数，具体消费数量根据所有订阅者request最小值为准。超过refCount后的订阅者不进行消费。
     *
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void refCountSubscribePublishRequest() throws InterruptedException {
        Flux<String> bridge = Flux.fromArray(new String[]{"start", "restart", "end"})
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"))
                .doOnCancel(() -> LOGGER.info("cancel now"));
        Flux<String> autoCo = bridge.publish().refCount(3);

        LOGGER.info("first subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("first subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"), subscription -> subscription.request(1));
        Thread.sleep(2000);
        LOGGER.info("second subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("second subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"), subscription -> subscription.request(2));

        Thread.sleep(2000);
        LOGGER.info("third subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("third subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("forth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("forth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("fifth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("fifth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        LOGGER.info("exit");
    }

    /**
     * 不同于publish，reply让所有的订阅者能够根据自己的需求决定消费多少消息，
     * 同时正常断开与动态源的连接，保证后续的订阅者正常消费
     *
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void refCountSubscribeReplyRequest() throws InterruptedException {
        Flux<String> bridge = Flux.fromArray(new String[]{"start", "restart", "end"})
                .doOnSubscribe(s -> LOGGER.info("subscribed to source"))
                .doOnTerminate(() -> LOGGER.info("disconnected now"))
                .doOnCancel(() -> LOGGER.info("cancel now"));
        Flux<String> autoCo = bridge.replay().refCount(2);

        LOGGER.info("first subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("first subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"), subscription -> subscription.request(1));
        Thread.sleep(2000);
        LOGGER.info("second subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("second subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"), subscription -> subscription.request(2));

        Thread.sleep(2000);
        LOGGER.info("third subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("third subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("forth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("forth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        Thread.sleep(2000);
        LOGGER.info("fifth subscriber : ");
        autoCo.subscribe(data -> LOGGER.info("fifth subscriber : " + data),
                System.err::println, () -> LOGGER.info("Done"));
        LOGGER.info("exit");
    }
}

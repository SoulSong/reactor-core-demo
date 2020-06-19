package com.shf.reactor;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author songhaifeng
 * @date 2017/11/3
 */
public class ConvertorDemo {

    @Test
    public void flux2Mono() {
        //通过from将Flux转换为Mono，取其第一个消息
        Mono.from(Flux.just("foo", "bar", "foobar")).subscribe(System.out::println);
    }
}

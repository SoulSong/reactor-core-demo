package com.shf.reactor;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

/**
 * Mono入门案例
 *
 * @author songhaifeng
 * @date 2017/10/11
 */
public class MonoDemo {

    /************************************静态创建Mono*********************************/

    @Test
    public void staticCreate() {
        //创建一个不包含任何元素，只发布结束消息的序列。
        Mono.empty().subscribe(System.out::println);

        //可以指定序列中包含的全部元素
        Mono.just("foo").subscribe(System.out::println);

        //从一个 Optional 对象或可能为 null 的对象中创建 Mono。只有 Optional 对象中包含值或对象不为 null 时，Mono 序列才产生对应的元素
        Mono.justOrEmpty(null).subscribe(System.out::println);
        Mono.justOrEmpty("justOrEmpty1").subscribe(System.out::println);
        Mono.justOrEmpty(Optional.of("justOrEmpty2")).subscribe(System.out::println);

        //创建一个只包含错误消息的序列
        Mono.error(new RuntimeException("error")).subscribe(System.out::println, System.err::println);

        //创建一个不包含任何消息通知的序列
        Mono.never().subscribe(System.out::println);

        //通过fromRunnable创建，并实现异常处理
        Mono.fromRunnable(() -> {
            System.out.println("thread run");
            throw new RuntimeException("thread run error");
        }).subscribe(System.out::println, System.err::println);

        //通过fromCallable创建
        Mono.fromCallable(() -> "callable run ").subscribe(System.out::println);

        //通过fromSupplier创建
        Mono.fromSupplier(() -> "create from supplier").subscribe(System.out::println);

        //创建一个 Mono 序列，在指定的延迟时间之后，产生数字 0 作为唯一值
        //测试发现消费发生线程并发主线程，通过Disposable获取消费状态，阻塞主线程至消费完成
        long start = System.currentTimeMillis();
        Disposable disposable = Mono.delay(Duration.ofSeconds(2)).subscribe(n -> {
            System.out.println("生产数据源：" + n);
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",生产到消费耗时：" + (System.currentTimeMillis() - start) + "ms");
        });
        System.out.println("主线程" + Thread.currentThread().getId() + "耗时：" + (System.currentTimeMillis() - start) + "ms");
        while (!disposable.isDisposed()) {
        }

    }

    /************************************动态创建Mono*********************************/

    @Test
    public void dynamicCreate() {
        //通过 create()方法来使用 MonoSink 来创建 Mono
        Mono.create(sink -> sink.success("create MonoSink")).subscribe(System.out::println);
    }
}

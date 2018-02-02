package org.zy.kafka.newclient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaConsumerDemo extends Thread {

    private final KafkaConsumer<String,String> consumer;

    private final String groupId;

    private ExecutorService messageProcessExecutor;

    private final String threadName;

    public KafkaConsumerDemo(String threadName , String topic , String groupId){
        this.setName(threadName);
        this.threadName = threadName;

        this.groupId = groupId;

        this.consumer = new KafkaConsumer<>(createProperties());

        // 订阅topic
        this.consumer.subscribe(Arrays.asList(topic));

        // 初始化线程池
        initThreadPool();
    }

    public Properties createProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        props.put("group.id",this.groupId);
        props.put("enable.auto.commit","true"); // 自动提交offset
        props.put("auto.commit.interval.ms","1000"); // 自动提交offset的时间间隔
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer",KafkaProperties.KEY_DESERIALIZER_STRING);
        props.put("value.deserializer",KafkaProperties.VALUE_DESERIALIZER_STRING);
        return props;
    }

    @Override
    public void run() {
        messageProcessExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    // 消费消息
                    for (ConsumerRecord<String,String> record : records){
                        System.out.println(String.format("%s consumer message partition=%s , offset=%s , key=%s , value=%s",
                                threadName,record.partition(),record.offset(),record.key(),record.value()));
                    }
                }

            }
        });
    }

    private void initThreadPool(){
        // 开启一个后台线程去处理消费
        messageProcessExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                String name = "kafka message process thread-" + threadNumber.getAndIncrement();
                Thread ret = new Thread(r, name);
                ret.setDaemon(true);
                return ret;
            }
        });
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }
}

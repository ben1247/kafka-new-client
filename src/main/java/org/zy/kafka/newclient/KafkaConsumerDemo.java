package org.zy.kafka.newclient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaConsumerDemo extends Thread {

    private final KafkaConsumer<String,String> consumer;

    private final String topic;

    private final String groupId;

    public KafkaConsumerDemo(String threadName , String topic , String groupId){
        this.setName(threadName);
        this.topic = topic;
        this.groupId = groupId;

        this.consumer = new KafkaConsumer<>(createProperties());

        // 订阅topic
        this.consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
    }

    public Properties createProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        props.put("group.id","test");
        props.put("enable.auto.commit","true"); // 自动提交offset
        props.put("auto.commit.interval.ms","1000");// 自动提交offset的时间间隔
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer",KafkaProperties.KEY_DESERIALIZER_INTEGER);
        props.put("value.deserializer",KafkaProperties.VALUE_DESERIALIZER_STRING);
        return props;
    }

    @Override
    public void run() {
        while (true){
            // 从服务器拉取消息，每次poll()可以拉取多个消息
            ConsumerRecords<String,String> records = consumer.poll(100);
            // 消费消息
            for (ConsumerRecord<String,String> record : records){
                System.out.printf("consumer message offset= %d , key = %s , value = %s\n",record.offset(),record.key(),record.value());
            }
        }
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }
}

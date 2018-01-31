package org.zy.kafka.newclient;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaProducerCallback implements Callback {

    private final long startTime; // 开始发送消息的时间戳
    private final int key; // 消息的key
    private final String message;  // 消息的value

    public KafkaProducerCallback(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 生产者成功发送消息，收到kafka服务端发来的ACK确认消息后，会调用此回调函数
     * @param metadata  生产者发送的消息的元数据，如果发送过程中出现异常，此参数为null
     * @param e 发送过程中出现的异常，如果发送成功，则此参数为null
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        long hostTime = System.currentTimeMillis() - startTime;
        if (metadata != null){
            // RecordMetadata中包含了分区信息、offset信息等
            System.out.println("message (" + key + ", " + message + ") sent to partition (" + metadata.partition() + "), offset(" + metadata.offset() + ") in " + hostTime + " ms");
        }else{
            e.printStackTrace();
        }
    }
}

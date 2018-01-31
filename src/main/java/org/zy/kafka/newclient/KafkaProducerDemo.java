package org.zy.kafka.newclient;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaProducerDemo extends Thread{

    private final KafkaProducer producer;

    private final String topic;

    private final boolean isAsync;

    /**
     *
     * @param topic
     * @param isAsync 消息发送方式：异步发送还是同步发送
     */
    public KafkaProducerDemo(String topic,boolean isAsync){
        final Properties props = new Properties();
        props.put("bootstrap.servers",KafkaProperties.BOOTSTRAP_SERVERS);
        props.put("client.id","kafkaProducerDemo"); // 客户端id
        props.put("key.serializer",KafkaProperties.KEY_SERIALIZER_INTEGER);
        props.put("value.serializer",KafkaProperties.VALUE_SERIALIZER_STRING);
        // producer需要server接收到数据之后发出的确认接收的信号，此项配置就是指procuder需要多少个这样的确认信号。此配置实际上代表了数据备份的可用性。以下设置为常用选项：
        //（1）acks=0： 设置为0表示producer不需要等待任何确认收到的信息。副本将立即加到socket  buffer并认为已经发送。没有任何保障可以保证此种情况下server已经成功接收数据，同时重试配置不会发生作用（因为客户端不知道是否失败）回馈的offset会总是设置为-1；
        //（2）acks=1： 这意味着至少要等待leader已经成功将数据写入本地log，但是并没有等待所有follower是否成功写入。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
        //（3）acks=all：这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
        props.put("acks", "1");
        this.producer = new KafkaProducer(props);

        this.topic = topic;

        this.isAsync = isAsync;
    }

    @Override
    public void run() {
        int messageNo = 31;
        while (messageNo <= 40){
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            if (isAsync){ // 异步发送消息
                // 第一个参数是ProducerRecord类型的对象，封装了目标Topic、消息的key、消息的value
                // 第二个参数是一个CallBack对象，当生产者接收到kafka发来的ACK确认消息的时候，会调用此CallBack对象的onCompletion()方法，实现回调功能
                producer.send(new ProducerRecord<>(this.topic,messageNo,messageStr),new KafkaProducerCallback(startTime,messageNo,messageStr));
            }else{ // 同步发送消息
                try {
                    producer.send(new ProducerRecord<>(this.topic,messageNo,messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            messageNo++;
        }
    }

    public KafkaProducer getProducer() {
        return producer;
    }
}

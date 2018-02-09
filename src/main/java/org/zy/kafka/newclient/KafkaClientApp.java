package org.zy.kafka.newclient;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaClientApp {

    public static void main(String[] args) throws InterruptedException {

        KafkaProducerDemo producer = new KafkaProducerDemo(KafkaProperties.TOPIC,false);
        producer.start();

        KafkaConsumerDemo consumer1 = new KafkaConsumerDemo("t1",KafkaProperties.TOPIC,KafkaProperties.GROUP_ID_1);
        consumer1.start();

        KafkaConsumerDemo consumer2 = new KafkaConsumerDemo("t2",KafkaProperties.TOPIC,KafkaProperties.GROUP_ID_2);
        consumer2.start();

        Thread.sleep(60000);

        // 关闭
//        producer.getProducer().close();
//        consumer1.getConsumer().close();
//        consumer2.getConsumer().close();
    }

}

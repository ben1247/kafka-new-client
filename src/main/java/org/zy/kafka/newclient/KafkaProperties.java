package org.zy.kafka.newclient;

/**
 * Created by yuezhang on 18/1/31.
 */
public class KafkaProperties {

//  public final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
//    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String BOOTSTRAP_SERVERS = "127.0.0.1:9095,127.0.0.1:9096,127.0.0.1:9097";

//    public final static String TOPIC = "my-kafka-1";
//    public final static String TOPIC = "kafka11-1";
    public final static String TOPIC = "new-replicated-3";

    public final static String GROUP_ID_1 = "myGroup1";
    public final static String GROUP_ID_2 = "myGroup2";




    public final static String KEY_SERIALIZER_STRING = "org.apache.kafka.common.serialization.StringSerializer";

    public final static String KEY_SERIALIZER_INTEGER = "org.apache.kafka.common.serialization.IntegerSerializer";

    public final static String VALUE_SERIALIZER_STRING = "org.apache.kafka.common.serialization.StringSerializer";

    public final static String KEY_DESERIALIZER_STRING = "org.apache.kafka.common.serialization.StringDeserializer";

    public final static String KEY_DESERIALIZER_INTEGER = "org.apache.kafka.common.serialization.IntegerDeserializer";

    public final static String VALUE_DESERIALIZER_STRING = "org.apache.kafka.common.serialization.StringDeserializer";


}

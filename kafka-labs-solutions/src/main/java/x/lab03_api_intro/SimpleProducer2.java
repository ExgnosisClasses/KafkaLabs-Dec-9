package x.lab03_api_intro;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.MyConfig;

public class SimpleProducer2 {
	private static final Logger logger = LoggerFactory.getLogger(SimpleProducer2.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    props.put("client.id", "SimpleProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

    String topic = "test";
    Integer key = new Integer(1);
    String value = "Hello world";
    ProducerRecord<Integer, String> record =
        new ProducerRecord<>(topic, key, value);
    // option 1 : fire and forget
    logger.debug("sending : " + record);
    producer.send(record);

    /*
    // option 2 : sync
    Future<RecordMetadata> future = producer.send(record);
    RecordMetadata recordMetaData = future.get();
    producer.send(record).get();

    // option 3 : async
    producer.send(record, new KafkaCallback2());
    */

    producer.close();

  }

}

class KafkaCallback2 implements Callback {
	private static final Logger logger = LoggerFactory.getLogger(KafkaCallback2.class);

  @Override
  public void onCompletion(RecordMetadata meta, Exception ex) {
    if (ex != null) // error
      ex.printStackTrace();

    if (meta != null) // success
      logger.debug("send success");
  }
}

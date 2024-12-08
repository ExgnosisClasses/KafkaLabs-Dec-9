package x.lab05_offsets;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.MyConfig;

public class GreetingsProducer {
	private static final Logger logger = LoggerFactory.getLogger(GreetingsProducer.class);
	
  public static final String TOPIC = "greetings";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    props.put("client.id", "GreetingsProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

    //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    for (int i = 1; i <= 10; i++) {
      Integer key = new Integer(i);
      //String value = df.format(new Date())+  "--" + i + ", Hello world";
      String value =  "Hello world";
      ProducerRecord<Integer, String> record =
          new ProducerRecord<>(TOPIC, key, value);
      logger.debug("sending : " + record);
      producer.send(record);
    }
    producer.close();

  }

}

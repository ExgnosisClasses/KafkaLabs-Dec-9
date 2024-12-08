package x.lab05_offsets;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.MyConfig;

public class SeekingConsumer {
	private static final Logger logger = LoggerFactory.getLogger(SeekingConsumer.class);
	private static final String TOPIC = "clickstream";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    props.put("group.id", "seeking1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    
    consumer.subscribe(Collections.singletonList(TOPIC)); // subscribe to

    logger.info("listening on  topic : " + TOPIC);

    TopicPartition partition = new TopicPartition(TOPIC, 0);

    int read = 0;
    while (read < 5) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(0));
      long position = consumer.position(partition);
      logger.debug("position " + position);
      for (ConsumerRecord<Integer, String> record : records) {
        read++;
        logger.debug("Received message : " + record);
        break; // only process first message
      }
      logger.debug("seeking to beginning of partition " + partition);
      consumer.seekToBeginning(Collections.singletonList(partition));

      // logger.debug("seeking to end of partition " + partition);
      // consumer.seekToEnd(Collections.singletonList(partition));

      // logger.debug("seeking to end -1 of partition " + partition);
      // consumer.seek(partition, position -1);

      //logger.debug("seeking to #5 of partition " + partition);
      //consumer.seek(partition, 5);
    }
    consumer.close();
  }
}

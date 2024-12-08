package x.lab03_api_intro;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import x.utils.MyConfig;

public class ClickstreamConsumer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClickstreamConsumer.class);

  private final String topic;
  private final KafkaConsumer<String, String> consumer;
  private final String groupId = "clickstream1";
  private boolean keepRunning = true;

  public ClickstreamConsumer(String topic) {
    this.topic = topic;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Clickstream-consumer");
	props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
	props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Arrays.asList(this.topic));
  }

  @Override
  public void run() {
    int numMessages = 0;
    while (keepRunning) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      int count = records.count();
      if (count == 0) continue;

      logger.debug("Got " + count + " messages"); 
      // iterate through records
      for (ConsumerRecord<String, String> record : records) {
         logger.debug("Received message: (" + record.key() + ", " +
         record.value() + ") at offset " + record.offset());
        numMessages++;
        logger.debug("Received message [" + numMessages + "] : " + record);
      }
    }

    logger.info("Received " + numMessages);
    //logger.info(this + " received " + numMessages);

    // close every thing
    consumer.close();
  }

  public void stop() {
    this.keepRunning = false;
    consumer.wakeup();
  }

  @Override
  public String toString() {
		return "ClickstreamConsumer (topic=" + this.topic + ", group=" + this.groupId + ")";
  }

  public static void main(String[] args) throws Exception {
    ClickstreamConsumer consumer = new ClickstreamConsumer("clickstream");
    Thread t1 = new Thread(consumer);
    logger.info("starting consumer... : " + consumer);
    t1.start();
    t1.join();
    logger.info("consumer shutdown.");

  }

}

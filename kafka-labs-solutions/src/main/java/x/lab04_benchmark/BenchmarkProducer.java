package x.lab04_benchmark;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.ClickStreamGenerator;
import x.utils.MyConfig;

enum SendMode {
  SYNC, ASYNC, FIRE_AND_FORGET
}

public class BenchmarkProducer implements Runnable, Callback {

	private static final Logger logger = LoggerFactory.getLogger(BenchmarkProducer.class);

  private final String topic;
  private final int maxMessages;
  private final SendMode sendMode;
  private final Properties props;

  private final KafkaProducer<Integer, String> producer;

  // topic, how many messages to send, and send mode
  public BenchmarkProducer(String topic, int maxMessages, SendMode sendMode) {
    this.topic = topic;
    this.maxMessages = maxMessages;
    this.sendMode = sendMode;

    this.props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    this.props.put("bootstrap.servers", "localhost:9092");
    this.props.put("client.id", "BenchmarkProducer");
    this.props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    this.props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  public void run() {

    int numMessages = 0;
    long t1, t2;
    long start = System.nanoTime();
    while ((numMessages < this.maxMessages)) {
      numMessages++;
      String clickstream = ClickStreamGenerator.getClickstreamAsCsv();
      // String clickstream = ClickStreamGenerator.getClickstreamAsJSON();
      ProducerRecord<Integer, String> record =
          new ProducerRecord<>(this.topic, numMessages, clickstream);
      t1 = System.nanoTime();
      try {
        switch (this.sendMode) {
        case FIRE_AND_FORGET:
          producer.send(record);
          break;
        case SYNC:
          producer.send(record).get();
          break;
        case ASYNC:
          producer.send(record, this);
          break;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      t2 = System.nanoTime();

      //logger.debug( "sent : [" + record + "] in " + (t2 - t1)  + " nano secs");
      // TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");

    }
    long end = System.nanoTime();
    producer.close(); // close connection

    // print summary
    logger.info(
        "== " + toString() + " done.  " + numMessages + " messages sent in "
            + (end - start) / 10e6 + " milli secs.  Throughput : "
            + numMessages * 10e9 / (end - start) + " msgs / sec");

  }

  @Override
  public String toString() {
    return "ClickstreamProducer (topic=" + this.topic + ", maxMessages="
        + this.maxMessages + ", sendMode=" + this.sendMode + ")";
  }

  // Kafka callback
  @Override
  public void onCompletion(RecordMetadata meta, Exception ex) {
    if (ex != null) {
      logger.error("Callback :  Error during async send");
      ex.printStackTrace();
    }
    if (meta != null) {
      //logger.debug("Callback : Success sending message " + meta);
    }

  }

  // test driver
  public static void main(String[] args) throws Exception {

    for (SendMode sendMode : SendMode.values()) {
      BenchmarkProducer producer =
          new BenchmarkProducer("benchmark", 100000, sendMode);
      logger.info("== Producer starting.... : " + producer);
      Thread t1 = new Thread(producer);
      t1.start();
      t1.join(); // wait for thread to complete
      logger.info("== Producer done.");
    }

  }

}

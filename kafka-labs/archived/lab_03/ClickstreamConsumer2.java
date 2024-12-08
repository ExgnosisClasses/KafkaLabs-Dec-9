package x.lab03_api_intro;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ClickstreamConsumer2 implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ClickstreamConsumer2.class);

	private final String topic;
	private final String groupId = "clickstream1";
	private final KafkaConsumer<String, String> consumer;
	private boolean keepRunning = true;
	NumberFormat formatter = NumberFormat.getInstance();

	public ClickstreamConsumer2(String topic) {
		this.topic = topic;
		Properties props = new Properties();
		// TODO-1 : set servers to "localhost:9092"
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "???");

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
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			int count = records.count();
			if (count == 0)
				continue;
			logger.debug("Got " + count + " messages");

			for (ConsumerRecord<String, String> record : records) {
				numMessages++;
				logger.debug("Received message [" + numMessages + "] : " + record);

			}
		}

		logger.info("Received " + formatter.format(numMessages));
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
		/*
		 * TODO-2 : create a consumer ClickstreamConsumer takes only one parameter name
		 * of topic to listen to. Set it to "clickstream"
		 */
		ClickstreamConsumer2 consumer = new ClickstreamConsumer2("???");

		Thread t1 = new Thread(consumer);
		logger.info("starting consumer... : " + consumer);
		t1.start();
		t1.join();
		logger.info("consumer shutdown.");

	}

}

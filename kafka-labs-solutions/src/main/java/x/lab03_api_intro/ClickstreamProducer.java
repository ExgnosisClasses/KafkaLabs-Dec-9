package x.lab03_api_intro;

import java.text.NumberFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.ClickStreamGenerator;
import x.utils.ClickstreamData;

public class ClickstreamProducer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClickstreamProducer.class);
	NumberFormat formatter = NumberFormat.getInstance();

	private final String topic;
	private final int maxMessages;
	private final int frequency;
	private final Properties props;
	private boolean keepRunning;

	private final KafkaProducer<String, String> producer;

	// topic, how many messages to send, and how often (in milliseconds)
	public ClickstreamProducer(String topic, int maxMessages, int frequency) {
		this.topic = topic;
		this.maxMessages = maxMessages;
		this.frequency = frequency;
		this.keepRunning = true;

		this.props = new Properties();
		this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "ClickstreamProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.producer = new KafkaProducer<>(props);
	}

	@Override
	public void run() {

		int numMessages = 0;
		long t1, t2;
		long start = System.nanoTime();
		while (this.keepRunning && (numMessages < this.maxMessages)) {
			numMessages++;
			ClickstreamData clickstream = ClickStreamGenerator.getClickStreamRecord();
			String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON(clickstream);
			ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, 
					clickstream.domain, clickstreamJSON);

			try {

				// Experiment: measure the time taken for both send options

				t1 = System.nanoTime();
				// sending without waiting for response
				producer.send(record);

				// sending and waiting for response
				// RecordMetadata meta = producer.send(record).get();
				t2 = System.nanoTime();

				logger.debug("sent : [" + record + "]  in " + formatter.format((t2 - t1) / 1e6) + " milli secs\n"); // TimeUnit.NANOSECONDS.toMillis(t2
																													// -
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			try {
				if (this.frequency > 0)
					Thread.sleep(this.frequency);
			} catch (InterruptedException e) {
			}
		}
		long end = System.nanoTime();
		producer.close(); // close connection

		// print summary
		logger.info("\n== " + toString() + " done.  " + numMessages + " messages sent in " + (end - start) / 10e6
				+ " milli secs.  Throughput : " + numMessages * 10e9 / (end - start) + " msgs / sec");

	}

	public void stop() {
		this.keepRunning = false;
	}

	@Override
	public String toString() {
		return "ClickstreamProducer (topic=" + this.topic + ", maxMessages=" + this.maxMessages + ", freq="
				+ this.frequency + " ms)";
	}

	// test driver
	public static void main(String[] args) throws Exception {

		ClickstreamProducer producer = new ClickstreamProducer("clickstream", 10, 100);
		logger.info("Producer starting.... : " + producer);
		Thread t1 = new Thread(producer);
		t1.start();
		t1.join(); // wait for thread to complete
		logger.info("Producer done.");

	}

}

package x.lab03_api_intro;

import java.text.NumberFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.ClickStreamGenerator;
import x.utils.ClickstreamData;

public class ClickstreamProducer2 implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClickstreamProducer2.class);

	private final String topic;
	private final int maxMessages;
	private final int frequency;
	private final Properties props;
	private boolean keepRunning;

	NumberFormat formatter = NumberFormat.getInstance();

	private final KafkaProducer<String, String> producer;

	// topic, how many messages to send, and how often (in milliseconds)
	public ClickstreamProducer2(String topic, int maxMessages, int frequency) {
		this.topic = topic;
		this.maxMessages = maxMessages;
		this.frequency = frequency;
		this.keepRunning = true;

		this.props = new Properties();
		// TODO-1 : set the broker to 'localhost:9092'
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "???");
	    
	    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ClickstreamProducer");
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	    		StringSerializer.class.getName());
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	    		StringSerializer.class.getName());		
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
			
			String key = clickstream.domain;
			String value = clickstreamJSON;

			// TODO-2 : let's construct a record
			// ProducerRecord takes three parameters 
                        //      - first param : topic = this.topic 
                        //      - second param : key = the key just contructed  (key)
                        //      - third param : value = the value just constructored (value)
			 
			ProducerRecord<String, String> record = null;
			//record = new ProducerRecord<>( ???, ???, ???);
			
			
			
			try {

				// Experiment: measure the time taken for both send options
				
				t1 = System.nanoTime(); 			
				// sending without waiting for response
				producer.send(record); 
				
				// sending and waiting for response
				//RecordMetadata meta = producer.send(record).get();
				t2 = System.nanoTime();
				
				logger.debug(String.format("Sent record [%d] (key:%s, value:%s), "
		        		+ "time took = %.2f ms",
		        		numMessages, key, value,	(t2-t1)/1e6));
				// TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms"
				
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

		// TODO-3 : end the Kafka producer. Call method 'close'
		// producer.???();

		// print summary
		logger.info("\n== " + toString() + " done.  " + formatter.format(numMessages) + " messages sent in "
				+ formatter.format((end - start) / 10e6) + " milli secs.  Throughput : "
				+ formatter.format(numMessages * 10e9 / (end - start)) + " msgs / sec");

	}

	public void stop() {
		this.keepRunning = false;
	}

	@Override
	public String toString() {
		return "ClickstreamProducer (topic=" + this.topic + ", maxMessages=" + formatter.format(this.maxMessages)
				+ ", freq=" + formatter.format(this.frequency) + " ms)";
	}

	// test driver
	public static void main(String[] args) throws Exception {
		ClickstreamProducer2 producer = null;
		
		// TODO-4 : create a new producer with the following params
        // ClickstreamProducer() takes three parameters 
        //      - first param : name of topic = "clickstream" 
        //      - second param : how many messages to send = 10 (start with 10 and increase later) 
        //      - third param : frequency, how often to send = 100 (in milliseconds, 0 for no wait between sends)
		
		//producer = 	new ClickstreamProducer(???, ??? , ???);
		 
		logger.info("Producer starting.... : " + producer); 
		Thread t1 = new Thread(producer); 
		t1.start(); 
		t1.join(); // wait for thread to complete
		logger.info("Producer done.");
	}

}

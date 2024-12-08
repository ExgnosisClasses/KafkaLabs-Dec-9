package x.lab06_domain_count;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.MyConfig;

public class DomainCountConsumer {
	private static final Logger logger = LoggerFactory.getLogger(DomainCountConsumer.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(MyConfig.TOPIC_CLICKSTREAM)); // subscribe

    Map<String, Integer> domainCount = new HashMap<>();
    Gson gson = new Gson();
    boolean keepRunning = true;
		while (keepRunning) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			if (records.count() == 0) continue;
			
			logger.debug("Got " + records.count() + " messages");
			for (ConsumerRecord<String, String> record : records) {
				try {
					logger.debug("Received message : " + record);

					// we can get teh domain from the key,
					//        String domain  = record.key();

					// here is also another way of getting domain
					// by parsing the JSON
					ClickstreamData clickstream =
					gson.fromJson(record.value(), ClickstreamData.class);
					String domain = clickstream.domain;

					// update count
					int currentCount =
					domainCount.getOrDefault(domain, 0);
					domainCount.put(domain, currentCount + 1);

					logger.debug("Domain Count is \n"
					+ Arrays.toString(domainCount.entrySet().toArray()));
				}catch (Exception ex) {
					logger.error("",ex);
				}
			} // end for
		} // end while
    consumer.close();
  }
}

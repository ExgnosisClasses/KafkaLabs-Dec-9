/*
package x.utils.archived;

import java.nio.file.Files;
import java.util.Properties;

public class RunLocalKafka {
  private static final String ZKHOST = "127.0.0.1";
  private static final int ZKPORT = 2181;
  private static final String KAFKA_HOST = "127.0.0.1";
  private static final String KAFKA_PORT = "9092";
//  private static final String TOPIC = "test";

  public static void main(String[] args) throws Exception {

    try {
      Properties zkProperties = new Properties();
      // zkProperties.setProperty("dataDir", "/tmp/zookeeper");
      zkProperties.setProperty("dataDir",
          Files.createTempDirectory("kafka-").toAbsolutePath().toString());
      zkProperties.setProperty("clientPort", "" + ZKPORT);

      Properties kafkaProperties = new Properties();
      kafkaProperties.setProperty("zookeeper.connect", ZKHOST + ":" + ZKPORT);
      kafkaProperties.setProperty("broker.id", "0");
      kafkaProperties.setProperty("log.dirs", "/tmp/kafka-logs");
      // kafkaProperties.setProperty("log.dirs",
      // Files.createTempDirectory("kafka-").toAbsolutePath().toString());

      kafkaProperties.setProperty("listeners",
          "PLAINTEXT://" + KAFKA_HOST + ":" + KAFKA_PORT);
      
      KafkaLocal kafka = new KafkaLocal(kafkaProperties, zkProperties);
      Thread.sleep(5000);
      kafka.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
*/

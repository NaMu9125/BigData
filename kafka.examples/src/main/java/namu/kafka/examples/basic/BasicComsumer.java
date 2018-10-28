package namu.kafka.examples.basic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BasicComsumer {

	public static void main(String[] args) {
		BasicComsumer c = new BasicComsumer();
		c.subcribe();
	}

	public void subcribe() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "mjseo-consumer");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "latest");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("test-topic"));
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			int i = 0;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
				for (ConsumerRecord<String, String> record : records) {
					//if (i++ % 100000 == 0) {
						end = System.currentTimeMillis() - start;
						System.out.printf("Topic: %s, Partition: %s, Offset: %d , Key: %s, Value: %s --> time : %d\n",
								record.topic(), record.partition(), record.offset(), record.key(), record.value(), end);
					//}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}


		System.out.println("END-" + end);

	}

}

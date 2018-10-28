package namu.kafka.examples.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BasicProducer p = new BasicProducer();
		p.send();
	}

	public void send() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "1");
		props.put("compression.type", "snappy");
		props.put("batch.size", "100000");
		props.put("timeout.ms", "50");

		Producer<String, String> producer = null;
		long start = System.currentTimeMillis();
		
		String[] keys = {"a","b","c"};
		String[] values = {"aaaaaaaa","bbbbbb","cccccc"};
		
		try {
			producer = new KafkaProducer<String, String>(props);

			for(long i = 0 ; i < 1000000000L ; i++) {
				
				producer.send(new ProducerRecord<String, String>("test-topic",keys[((int)(i%3))], values[((int)(i%3))]));
				//Thread.sleep(10);
				if(i % 100000 == 0)
					System.out.println("index " + i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
		long end = System.currentTimeMillis() - start;
		
		System.out.println("END-" + end);

	}

}

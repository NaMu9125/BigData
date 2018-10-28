package namu.flink.examples.wordcount;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

public class KafkaWindowWordCount {

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test-consumer");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "latest");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>("test-topic", new SimpleStringSchema(), props));

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = stream.flatMap(new FlatMapFunction<String, WordWithCount>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<WordWithCount> out) {
				for (String word : value.split("\\s")) {
					out.collect(new WordWithCount(word, 1L));
				}
			}
		}).keyBy("word").timeWindow(Time.seconds(1)).reduce(new ReduceFunction<WordWithCount>() {
			private static final long serialVersionUID = 1L;

			@Override
			public WordWithCount reduce(WordWithCount a, WordWithCount b) {
				return new WordWithCount(a.word, a.count + b.count);
			}
		});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(3);

		env.execute("Socket Window WordCount");
	}

	// Data type for words with count
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
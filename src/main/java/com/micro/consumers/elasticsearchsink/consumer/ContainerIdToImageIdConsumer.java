package com.micro.consumers.elasticsearchsink.consumer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
import com.micro.kafka.ConsumerThread;
import com.micro.kafka.KafkaConsumer;
public class ContainerIdToImageIdConsumer{
	private ElasticSearchClient client = null;
	private Consumer consumer;
	public ContainerIdToImageIdConsumer(ElasticSearchClient client,Properties config, String topic) {
		this.client = client;
		KafkaConsumer
		.build()
		.withConfig(config)
		.withTopic(topic)
		.withProcessor(()->{
			if(consumer==null)consumer=KafkaConsumer.builder.getConsumer();
			return execute(client);
			})
		.consume();

	}

	private boolean execute(ElasticSearchClient client) {
		ConsumerRecords<String, String> records;
		synchronized (consumer) {
				 records = consumer.poll(100);	
			}
		for (ConsumerRecord<String, String> record : records) {
			Map<String, Object> map = new HashMap<>();
			map.put(Constants.DOCKERHOST, record.key());
			map.put(Constants.IMAGEID, record.value());
			map.put(Constants.TIMESTAMP,new Date(record.timestamp()));
			IndexRequest indexRequest = new IndexRequest(Constants.DOCKERX_CONTAINERID_TO_IMAGEID_INDEX,Constants.TYPE).id(record.key()).source(map);
			try {
				IndexResponse indexResponse = client.getClient().index(indexRequest, RequestOptions.DEFAULT);
				System.out.println(indexResponse.getId());
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
					+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
		}
		return true;
	}
}

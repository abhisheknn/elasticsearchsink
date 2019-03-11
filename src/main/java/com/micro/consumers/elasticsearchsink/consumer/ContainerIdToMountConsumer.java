package com.micro.consumers.elasticsearchsink.consumer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
import com.micro.kafka.KafkaConsumer;
public class ContainerIdToMountConsumer {
	private ElasticSearchClient client = null;
	private Gson gson = new Gson();
	private Type mapType = new TypeToken<Map<String, Object>>() {
	}.getType();
	private Type listType = new TypeToken<List<Map<String, Object>>>() {
	}.getType();
	
	private KafkaConsumer kafkaConsumer= new KafkaConsumer();
	
	public ContainerIdToMountConsumer(ElasticSearchClient client,Properties config, String topic) {
		this.client = client;
		kafkaConsumer
		.build()
		.withConfig(config)
		.withTopic(topic)
		.withProcessor(()->{
			this.execute(client);
			})
		.consume();

	}
	
	
	

	private boolean execute(ElasticSearchClient client) {
		
		ConsumerRecords<String, String> records  = kafkaConsumer.builder.getConsumer().poll(100);	
			
			for (ConsumerRecord<String, String> record : records) {
				try {
					Map<String, Object> map = gson.fromJson(record.value(), mapType);
					map.put(Constants.DOCKERHOST, record.key());
					map.put(Constants.TIMESTAMP,new Date(record.timestamp()));
					IndexRequest indexRequest = new IndexRequest(Constants.DOCKERX_CONTAINERID_TO_MOUNT_INDEX,Constants.TYPE).source(map);

					IndexResponse indexResponse = client.getClient().index(indexRequest, RequestOptions.DEFAULT);
					System.out.println(indexResponse.getId());
				} catch (IOException e) {
					e.printStackTrace();
				}catch(Exception e) {
					e.printStackTrace();
				}
				System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
						+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
			}
			
			return true;
	}
}

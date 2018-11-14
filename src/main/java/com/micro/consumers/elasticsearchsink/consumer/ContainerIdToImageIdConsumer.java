package com.micro.consumers.elasticsearchsink.consumer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
public class ContainerIdToImageIdConsumer extends ConsumerThread {
	private ElasticSearchClient client = null;

	private Gson gson = new Gson();
	Type mapType = new TypeToken<Map<String, Object>>() {
	}.getType();
	Type listType = new TypeToken<List<Map<String, Object>>>() {
	}.getType();

	public ContainerIdToImageIdConsumer(ElasticSearchClient client, String brokers, String groupId, String topic) {
		super(brokers, groupId, topic);
		this.client = client;
	}

	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				Map<String, Object> map = new HashMap<>();
				map.put(Constants.DOCKERHOST, record.key());
				map.put(Constants.IMAGEID, record.value());
				map.put(Constants.TIMESTAMP,new Date(record.timestamp()));
				IndexRequest indexRequest = new IndexRequest(Constants.DOCKERX_CONTAINERID_TO_IMAGEID_INDEX,Constants.DOCKERX_CONTAINERID_TO_IMAGEID).id(record.key()).source(map);
				try {
					IndexResponse indexResponse = client.getClient().index(indexRequest, RequestOptions.DEFAULT);
					System.out.println(indexResponse.getId());
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
						+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
			}
		}

	}

}

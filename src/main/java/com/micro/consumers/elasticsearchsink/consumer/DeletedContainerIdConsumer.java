package com.micro.consumers.elasticsearchsink.consumer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
import com.micro.kafka.KafkaConsumer;

public class DeletedContainerIdConsumer{
	private ElasticSearchClient client = null;
	private Consumer consumer;
	private Gson gson = new Gson();
	Type mapType = new TypeToken<Map<String, Object>>() {
	}.getType();
	Type listType = new TypeToken<List<Map<String, Object>>>() {
	}.getType();
	private  KafkaConsumer kafkaConsumer= new KafkaConsumer();
	public DeletedContainerIdConsumer(ElasticSearchClient client, Properties config, String topic) {
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
		ConsumerRecords<String, String> records;
		records = kafkaConsumer.builder.getConsumer().poll(100);
			for (ConsumerRecord<String, String> record : records) {
				try {
				Map<String,Object> map=	gson.fromJson(record.value(), mapType);
				List<String> deletedContainersIds=(List<String>)map.get("value");
				for(String deletedContainerId:deletedContainersIds) {
				addToDeletedContainersIndex(record, deletedContainerId);
				deleteFromContainersIndex(record, deletedContainerId);
				deleteFromContainerIdToImageIdIndex(record, deletedContainerId);
				deleteFromContaineridToMountIndex(record, deletedContainerId);
				}
				System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
						+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			}
			return true;
		}

	private void addToDeletedContainersIndex(ConsumerRecord<String, String> record, String deletedContainerId) throws IOException {
		
		GetRequest getRequest = new GetRequest(Constants.DOCKERX_CONTAINER_INDEX, Constants.TYPE,
				record.key() + "_" + deletedContainerId);
		GetResponse getResponse = client.getClient().get(getRequest, RequestOptions.DEFAULT); 
		Map<String, Object> source = getResponse.getSource();
		if(source!=null) {
		IndexRequest indexRequest = new IndexRequest(Constants.DOCKERX_DELETED_CONTAINERS_INDEX,
				Constants.TYPE).id(record.key() + "_" + deletedContainerId).source(source);
		IndexResponse indexResponse = client.getClient().index(indexRequest, RequestOptions.DEFAULT);
		}
		
	}

	private void deleteFromContainersIndex(ConsumerRecord<String, String> record, String deletedContainerId) throws IOException {
		
DeleteRequest requestForContainers = new DeleteRequest(Constants.DOCKERX_CONTAINER_INDEX,Constants.TYPE,record.key()+"_" + deletedContainerId);
DeleteResponse deleteResponseForContainers = client.getClient().delete(
			requestForContainers, RequestOptions.DEFAULT);
	}

	private void deleteFromContainerIdToImageIdIndex(ConsumerRecord<String, String> record, String deletedContainerId) throws IOException {
		
DeleteRequest requestForMounts = new DeleteRequest(Constants.DOCKERX_CONTAINERID_TO_IMAGEID_INDEX,Constants.TYPE,record.key()+"_" + deletedContainerId);
DeleteResponse deleteResponseForMounts = client.getClient().delete(
			requestForMounts, RequestOptions.DEFAULT);
		
	}

	private void deleteFromContaineridToMountIndex(ConsumerRecord<String, String> record, String deletedContainerId) throws IOException {
		DeleteByQueryRequest request = new DeleteByQueryRequest(Constants.DOCKERX_CONTAINERID_TO_MOUNT_INDEX);
		request.setQuery(new TermQueryBuilder(Constants.CONTAINERID, record.key() + "_" + deletedContainerId));
		
			BulkByScrollResponse bulkResponse = client.getClient().deleteByQuery(request,
					RequestOptions.DEFAULT);
	}

}

package com.micro.consumers.elasticsearchsink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
import com.micro.consumers.elasticsearchsink.consumer.ConsumerGroup;
import com.micro.consumers.elasticsearchsink.consumer.ConsumerThread;
import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToImageIdConsumer;
import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToMountConsumer;
//import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToImageIdConsumer;
//import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToMountConsumer;
import com.micro.consumers.elasticsearchsink.consumer.ContainerListToStreamConsumer;
import com.micro.consumers.elasticsearchsink.consumer.DeletedContainerIdConsumer;




@SpringBootApplication
@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
public class ElasticSearchSink {


	
	public static void main(String[] args) {
		SpringApplication.run(ElasticSearchSink.class, args);
		spwanContainerListToStreamConsumer();
		spwanContainerIdToImageIdConsumer();
		spwanContainerIdToMountConsumer();
		spwanDeletedContainerIdConsumer();
	}

	private static void spwanContainerListToStreamConsumer() {
		ElasticSearchClient client =new ElasticSearchClient(); 
		ConsumerThread ncThread=     new ContainerListToStreamConsumer(client,Constants.KAFKABROKER, Constants.CONTAINER_LIST_TO_STREAM_CONSUMER_GROUP_ID, Constants.CONTAINER_LIST_TO_STREAM);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
	
	private static void spwanContainerIdToImageIdConsumer() {
		ElasticSearchClient client= new ElasticSearchClient(); 
		ConsumerThread ncThread=     new ContainerIdToImageIdConsumer(client,Constants.KAFKABROKER, Constants.CONTAINERID_TO_IMAGEID_CONSUMER_GROUP_ID, Constants.CONTAINERID_TO_IMAGEID_TOPIC);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
	
	private static void spwanContainerIdToMountConsumer() {
		ElasticSearchClient client= new ElasticSearchClient(); 
		ConsumerThread ncThread=     new ContainerIdToMountConsumer(client,Constants.KAFKABROKER, Constants.CONTAINER_TO_MOUNT_CONSUMER_GROUP_ID, Constants.CONTAINER_TO_MOUNTS_TOPIC);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
	
	
	private static void spwanDeletedContainerIdConsumer() {
		ElasticSearchClient client= new ElasticSearchClient(); 
		ConsumerThread ncThread=     new DeletedContainerIdConsumer(client,Constants.KAFKABROKER, Constants.DELETED_CONTAINERIDS_GROUPID, Constants.DELETED_CONTAINERIDS_TOPICS);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
}

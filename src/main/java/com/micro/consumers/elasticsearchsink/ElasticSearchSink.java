package com.micro.consumers.elasticsearchsink;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import com.micro.consumers.elasticsearchsink.common.Constants;
import com.micro.consumers.elasticsearchsink.connection.ElasticSearchClient;
import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToImageIdConsumer;
import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToMountConsumer;
//import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToImageIdConsumer;
//import com.micro.consumers.elasticsearchsink.consumer.ContainerIdToMountConsumer;
import com.micro.consumers.elasticsearchsink.consumer.ContainerListToStreamConsumer;
import com.micro.consumers.elasticsearchsink.consumer.DeletedContainerIdConsumer;
import com.micro.kafka.ConsumerGroup;
import com.micro.kafka.ConsumerThread;

@SpringBootApplication
@EnableAutoConfiguration(exclude = { MongoAutoConfiguration.class })
public class ElasticSearchSink {

	public static void main(String[] args) {
		SpringApplication.run(ElasticSearchSink.class, args);
		 spwanContainerListToStreamConsumer();
		 spwanContainerIdToImageIdConsumer();
		 spwanContainerIdToMountConsumer();
		 spwanDeletedContainerIdConsumer();
	}

	private static void spwanContainerListToStreamConsumer() {
		ElasticSearchClient client = new ElasticSearchClient();
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKABROKER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONTAINER_LIST_TO_STREAM_CONSUMER_GROUP_ID);

		new ContainerListToStreamConsumer(client, properties, Constants.CONTAINER_LIST_TO_STREAM);

	}

	private static void spwanContainerIdToImageIdConsumer() {
		ElasticSearchClient client = new ElasticSearchClient();
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKABROKER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONTAINERID_TO_IMAGEID_CONSUMER_GROUP_ID);
		new ContainerIdToImageIdConsumer(client, properties, Constants.CONTAINERID_TO_IMAGEID_TOPIC);
	}

	private static void spwanContainerIdToMountConsumer() {
		ElasticSearchClient client = new ElasticSearchClient();
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKABROKER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONTAINER_TO_MOUNT_CONSUMER_GROUP_ID);
		new ContainerIdToMountConsumer(client, properties, Constants.CONTAINER_TO_MOUNTS_TOPIC);

	}

	private static void spwanDeletedContainerIdConsumer() {
		ElasticSearchClient client = new ElasticSearchClient();
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKABROKER);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.DELETED_CONTAINERIDS_GROUPID);
		new DeletedContainerIdConsumer(client, properties, Constants.DELETED_CONTAINERIDS_TOPICS);

	}
}

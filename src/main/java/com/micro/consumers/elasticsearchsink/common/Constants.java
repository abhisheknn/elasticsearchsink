package com.micro.consumers.elasticsearchsink.common;

public class Constants {

	public static final String CONTAINER_DETAILS_TOPIC = "dockerx.container_details";
	public static final String CONTAINER_LIST_TO_STREAM = "dockerx.container_list_to_stream";
	public static final String CONTAINERID_TO_IMAGEID_TOPIC = "dockerx.containerid_to_imageid";
	public static final String CONTAINER_TO_MOUNTS_TOPIC = "dockerx.container_to_mount";
	public static final String CONTAINER_DETAILS_CONSUMER_GROUP_ID = "container_details_consumer";
	public static final String CONTAINERID_TO_IMAGEID_CONSUMER_GROUP_ID = "container_to_image_id_consumer";
	public static final String CONTAINER_TO_MOUNT_CONSUMER_GROUP_ID = "container_to_mount_id_consumer";
	public static final String KAFKABROKER = System.getenv("KAFKABROKER");
//	public static final String MONGODB_DATABASE = System.getenv("MONGODB_DATABASE");
//	public static final String MONGODBUSERNAME = System.getenv("MONGODBUSERNAME");
//	public static final String MONGODBPASSWORD = System.getenv("MONGODBPASSWORD");
	public static final int ELASTICSEARCHPORT = Integer.parseInt(System.getenv("ELASTICSEARCHPORT"));
	public static final String ELASTICSEARCHHOST = System.getenv("ELASTICSEARCHHOST");
	public static final String CONTAINER_LIST_TO_STREAM_CONSUMER_GROUP_ID = "container_list_to_stream_consumer";
	
	// Elasticsearch indexes
	public static final String DOCKERX_CONTAINER_INDEX = "dockerx_container";
	public static final String DOCKERX_CONTAINERID_TO_IMAGEID_INDEX = "dockerx_containerid_to_imageid";
	public static final String DOCKERX_CONTAINERID_TO_MOUNT_INDEX = "dockerx_containerid_to_mount";
	public static final String DOCKERX_DELETED_CONTAINERS_INDEX = "dockerx_deleted_containers";
	
	//Elasticsearch Type
	
	public static final String TYPE="_doc";
	
//	public static final String DOCKERX_CONTAINER = "container";
//	public static final String DOCKERX_CONTAINERID_TO_IMAGEID = "image";
//	public static final String DOCKERX_CONTAINERID_TO_MOUNT = "mount";
	public static final String DOCKERHOST = "dockerhost";
	public static final String IMAGEID = "imageid";
	public static final String TIMESTAMP = "timestamp";
	public static final String CONTAINERID = "CONTAINERID";
	public static final String DELETED_CONTAINERIDS_GROUPID = "deleted_containerid_list";
	public static final String DELETED_CONTAINERIDS_TOPICS = "dockerx.deleted_containerid_list";
	
	

}

package com.micro.consumers.elasticsearchsink.connection;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.kenai.constantine.Constant;
import com.micro.consumers.elasticsearchsink.common.Constants;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

@Component
@Scope("singletone")
public class ElasticSearchClient {

	private String hostName = Constants.ELASTICSEARCHHOST;
	private int port = Constants.ELASTICSEARCHPORT;
	private String userName = "";
	private String password = "";
	private RestHighLevelClient client = null;
	final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

	public ElasticSearchClient() {
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, port, "http"));
//				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//
//					@Override
//					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//					}
//				});

		client = new RestHighLevelClient(builder);
	}

	public RestHighLevelClient getClient() {
		return client;
	}

}

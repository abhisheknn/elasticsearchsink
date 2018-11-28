package com.micro.consumers.elasticsearchsink.connection;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


import com.micro.consumers.elasticsearchsink.common.Constants;


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

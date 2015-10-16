package com.tliu.demo.esclient

import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

public class EsClientFactoryTest
{
	private final Logger logger = LoggerFactory.getLogger(this.class)

	EsClientFactory esClientFactory

	@Before
	public void before()
	{
		// You may want to change the port number to match your Elasticsearch
		esClientFactory = new EsClientFactory("elasticsearch", "localhost", 9335)
	}

	@Test
	public void test()
	{
		Client client = esClientFactory.getClient();

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9335));

		String jsonQuery = """{"query":{"filtered":{"filter":{"term":{"_id":"1"}}}}}"""
//		logger.debug("JsonQuery is {}", jsonQuery)
//
//		logger.debug("Before SearchRequestBuilder...");

//		jsonQuery = """{"size":"100","query":{"indices":{"no_match_query":"none","indices":["patient_centric_1"],"query":{"nested":{"path":"LB","query":{"filtered":{"filter":{"bool":{"must":[{"term":{"LB.LBTESTCD":"GLUC"}},{"script":{"script":"strORRES = doc[fieldName].value; if (strORRES == null) return false; else return strORRES.toInteger() < fieldValue;","params":{"fieldName":"LB.LBORRES","fieldValue":90}}},{"range":{"LB.LBSTDY":{"from":98,"to":104}}}]}}}}}}}},"post_filter":{"query":{"nested":{"path":"LB","query":{"filtered":{"filter":{"bool":{"must":[{"term":{"LB.LBSPEC":"SALIVA"}},{"range":{"LB.LBSTDY":{"from":70,"to":104}}},{"term":{"LB.LBANTREG":"LIVER"}}]}}}}}}}}"""
		SearchRequestBuilder request = client.prepareSearch("twitter")
				.setTypes("data")
				.setSource(jsonQuery)
//				.setExtraSource(jsonQuery)
//				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.termFilter("_id", "100")))

//		logger.debug("After SearchRequestBuilder, request is: {}", request);

		SearchResponse response = request.execute().actionGet()

		logger.debug("Response: {}", response)
		logger.debug("Response.toString: {}", response.toString())
	}

	@Test
	public void test2()
	{
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9335));

//		[1, 2, 3, 4].each() {
//			String body = """{"name":"Hello world ${it}"}"""
//			IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client, "twitter")
//					.setType("data")
//					.setId(it.toString())
//					.setSource(body)
//					.setRefresh(true)
//			IndexResponse indexResponse = indexRequestBuilder.execute().actionGet()
//		}

		String jsonQuery = """{"query":{"filtered":{"filter":{"term":{"_id":"1"}}}}}"""

		// use local Client
		SearchRequestBuilder requestBuilder = client.prepareSearch("twitter")
				.setTypes("data")
				.setSource(jsonQuery)
		SearchResponse response = requestBuilder.execute().actionGet()
		println response.toString()

		// use EsClientFactory client
		client = esClientFactory.getClient()
		SearchRequestBuilder requestBuilder2 = client.prepareSearch("twitter")
				.setTypes("data")
				.setSource(jsonQuery)

		println requestBuilder2.toString()

		SearchResponse response2 = requestBuilder2.execute().actionGet()
		println response2.toString()
	}
}

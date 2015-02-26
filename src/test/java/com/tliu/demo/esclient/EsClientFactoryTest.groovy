package com.tliu.demo.esclient

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.QueryBuilders
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
		esClientFactory = new EsClientFactory("elasticsearch", "localhost", 9335)
	}

	@Test
	public void test()
	{
		Client client = esClientFactory.getClient();

		String query = """{"filtered":{"query":{"match_all":{}},"filter":{"term":{"_id":"100"}}}}"""

		logger.debug("Query: {}", query)

		SearchRequestBuilder request = client.prepareSearch("patient_centric_1")
				.setTypes("patient_data")
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.termFilter("_id", "100")))

		logger.debug("Request: {}", request)

		SearchResponse response = request.execute().actionGet()

		logger.debug("Response: {}", response)
	}
}

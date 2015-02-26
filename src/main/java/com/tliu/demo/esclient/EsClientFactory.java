package com.tliu.demo.esclient;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class EsClientFactory
{
	private final Client client;

	public EsClientFactory(String clusterName, String hostname, int port)
	{
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostname, port));
	}

	public Client getClient()
	{
		return client;
	}

	public void shutdown()
	{
		client.close();
	}
}

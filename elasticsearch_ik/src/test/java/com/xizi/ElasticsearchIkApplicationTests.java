package com.xizi;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.InetAddress;


@SpringBootTest
class ElasticsearchIkApplicationTests {

    private PreBuiltTransportClient preBuiltTransportClient;


    //创建ES客户端操作对象
    @Before
    public void init() throws Exception {
         preBuiltTransportClient = new PreBuiltTransportClient(Settings.EMPTY);
        preBuiltTransportClient.addTransportAddress(
                new TransportAddress(InetAddress.getByName("192.168.153.124"),9300));
    }

    @Test
    public void test3() throws Exception {
        preBuiltTransportClient = new PreBuiltTransportClient(Settings.EMPTY);
        preBuiltTransportClient.addTransportAddress(
                new TransportAddress(InetAddress.getByName("192.168.153.124"),9300));
    }

    //创建索引
    @Test
    public void createIndex() throws Exception {

        //执行索引创建
        CreateIndexResponse createIndexResponse = preBuiltTransportClient.admin().indices().prepareCreate("xizi666").get();
        System.out.println(createIndexResponse.isAcknowledged());
    }

    @After
    public void after(){
        preBuiltTransportClient.close();
    }




}

package com.xizi.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

@Configuration
public class RestClientConfig extends AbstractElasticsearchConfiguration {

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        //定义客户端配置对象
        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo("192.168.153.124:9201","192.168.153.124:9202","192.168.153.124:9203")
                .build();
        //通过RestClient对象创建
        return RestClients.create(clientConfiguration).rest();
    }

}
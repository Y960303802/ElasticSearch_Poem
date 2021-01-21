package com.xizi;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
class ElasticsearchSpringbootApplicationTests {

    @Qualifier("elasticsearchClient")
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void test() throws Exception {
        //测试删除
        DeleteRequest deleteRequest = new DeleteRequest("xizi","emp","4");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    @Test
    public void  test1() throws IOException {
        //创建一条文档
        IndexRequest indexRequest = new IndexRequest("ems", "emp", "12");
        IndexRequest source = indexRequest.source("{\"name\":\"戏子\",\"age\":21}", XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(source, RequestOptions.DEFAULT);
        System.out.println(index.status());
    }

    @Test
    public void testSearch() throws IOException {
        //创建搜索对象
        SearchRequest searchRequest = new SearchRequest("ems");
        //搜索资源+
        // 构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("content","spring"))
                .from(0) //起始条数
                .size(20)  //每页展示数
                .postFilter(QueryBuilders.matchAllQuery())  //过滤条件
                .sort("age", SortOrder.DESC)  //排序
                .highlighter(new HighlightBuilder().field("*").requireFieldMatch(false).preTags("<span style='color:red'>").postTags("</span>")); //高亮

        searchRequest.types("emp").source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总文档数量："+searchResponse.getHits().getTotalHits());
        System.out.println("文档的最大得分： "+searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsMap());
            System.out.println("高亮之后"+hit.getHighlightFields());
        }
    }


    @Test
    public void  testUpdate() throws IOException {
        //更新
        UpdateRequest updateRequest = new UpdateRequest("ems","emp","12");
        updateRequest.doc("{\"name\":\"戏子666\",\"age\":21}",XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    @Test
    public void  testBulk() throws IOException {
        //批量操作
        BulkRequest bulkRequest = new BulkRequest();
        //添加
        IndexRequest indexRequest = new IndexRequest("ems", "emp", "11");
         indexRequest.source("{\"name\":\"戏子\",\"age\":21}", XContentType.JSON);
        //删除
        DeleteRequest deleteRequest = new DeleteRequest("ems","emp","11");
        //修改
        UpdateRequest updateRequest = new UpdateRequest("ems","emp","12");
        updateRequest.doc("{\"name\":\"戏子777\",\"age\":21}",XContentType.JSON);
        bulkRequest.add(indexRequest);
        bulkRequest.add(deleteRequest);
        bulkRequest.add(updateRequest);
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulk) {
            System.out.println(bulkItemResponse.status());
        }
    }

    @Test
    public void search() throws IOException {
        //查询所有
        SearchRequest searchRequest = new SearchRequest("ems");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.types("emp").source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


}

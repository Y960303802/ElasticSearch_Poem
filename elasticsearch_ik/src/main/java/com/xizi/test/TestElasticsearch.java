package com.xizi.test;


import com.alibaba.fastjson.JSONObject;
import com.xizi.pojo.Emp;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

public class TestElasticsearch {

    private PreBuiltTransportClient preBuiltTransportClient;


    //创建ES客户端操作对象
    @Before
    public void init() throws Exception {
        preBuiltTransportClient = new PreBuiltTransportClient(Settings.EMPTY);
        preBuiltTransportClient.addTransportAddress(
                new TransportAddress(InetAddress.getByName("192.168.153.124"),9300));
    }

    //创建索引
    @Test
    public void createIndex() throws Exception {
        //执行索引创建
        CreateIndexResponse createIndexResponse = preBuiltTransportClient.admin().indices().prepareCreate("xizi777").get();
        System.out.println(createIndexResponse.isAcknowledged());
    }

    //删除索引
    @Test
    public void deleteIndex() throws Exception {
        //执行索引创建
        AcknowledgedResponse xizi777 = preBuiltTransportClient.admin().indices().prepareDelete("xizi777").get();
        System.out.println(xizi777.isAcknowledged());
    }




    //创建索引类型和映射
    @Test
    public void createIndexAndMapper() throws Exception {
        //创建索引
        CreateIndexRequest xizi777 = new CreateIndexRequest("xizi777");
        //定义json格式映射
        String json = "{\"properties\":{\"name\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"},\"age\":{\"type\":\"integer\"},\"sex\":{\"type\":\"keyword\"},\"content\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"}}}";
        //设置类型和mapping
        xizi777.mapping("emp",json, XContentType.JSON);
        //执行创建
        CreateIndexResponse createIndexResponse = preBuiltTransportClient.admin().indices().create(xizi777).get();
        System.out.println(createIndexResponse.isAcknowledged());
    }



    //索引中创建文档 指定id
    @Test
    public void createIndexOptionId() throws Exception {
        Emp emp = new Emp("小尹", 21, "男", "这是一个单纯的少年");
        String empString = JSONObject.toJSONString(emp);
        IndexResponse indexResponse = preBuiltTransportClient.prepareIndex("xizi777", "emp", "1").setSource(empString, XContentType.JSON).get();
        System.out.println(indexResponse.status());
    }

    //索引中创建文档 自动生成id
    @Test
    public void createIndexAndDocument() throws Exception {
        Emp emp = new Emp("小尹", 18, "男", "这是一个单纯的少年");
        String empString = JSONObject.toJSONString(emp);
        IndexResponse indexResponse = preBuiltTransportClient.prepareIndex("xizi777", "emp").setSource(empString, XContentType.JSON).get();
        System.out.println(indexResponse.status());
    }


    //更新一条记录
    @Test
    public void testUpdate() throws Exception {
        Emp emp = new Emp();
        emp.setName("小尹666");
        String s = JSONObject.toJSONString(emp);
        UpdateResponse updateResponse = preBuiltTransportClient.prepareUpdate("xizi777", "emp", "1")
                .setDoc(s,XContentType.JSON).get();
        System.out.println(updateResponse.status());
    }






    //删除一条记录
    @Test
    public void  testDelete() throws Exception {
        DeleteResponse deleteResponse = preBuiltTransportClient.prepareDelete("xizi777", "emp", "1").get();
        System.out.println(deleteResponse.status());
    }

    //查询一条
    @Test
    public void testFindOne(){
        GetResponse documentFields = preBuiltTransportClient.prepareGet("xizi777", "emp", "1").get();
        System.out.println(documentFields.getSourceAsString());
    }





/**
 * 查询所有并排序
 *  addSort("age", SortOrder.ASC)  指定排序字段以及使用哪种方式排序
 *  addSort("age", SortOrder.DESC) 指定排序字段以及使用哪种方式排序
 */
    @Test
    public void testMatchAllQuery() throws Exception {
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp")
                .setQuery(QueryBuilders.matchAllQuery()).addSort("age", SortOrder.DESC).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        System.out.println("最大得分："+hits.getMaxScore());
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println(", 指定字段结果:"+hit.getSourceAsMap().get("name"));
            System.out.println("=================================================");
        }
    }




     // term查询
    @Test
    public void testTerm() throws Exception {
        //创建term查询条件
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("name","尹");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(queryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        System.out.println("最大得分："+hits.getMaxScore());
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }



    // rang查询
    @Test
    public void testRange() throws Exception {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").lt(45).gte(8);
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(rangeQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        System.out.println("最大得分："+hits.getMaxScore());
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }



    //批量操作
    @Test
    public void testBulk() throws Exception {
        //添加第一条记录
        IndexRequest request1 = new IndexRequest("xizi777","emp","6");
        Emp emp = new Emp("戏子665", 21, "男", "牛逼");
        request1.source(JSONObject.toJSONString(emp),XContentType.JSON);
        //添加第二条记录
        IndexRequest request2 = new IndexRequest("xizi777","emp","7");
        Emp emp2 = new Emp("伟少666", 21, "女", "牛逼");
        request2.source(JSONObject.toJSONString(emp2),XContentType.JSON);
        //更新记录
        UpdateRequest updateRequest = new UpdateRequest("xizi777","emp","4");
        Emp empUpdate = new Emp();
        empUpdate.setName("伟少牛逼");
        updateRequest.doc(JSONObject.toJSONString(empUpdate),XContentType.JSON);
        //删除一条记录
//        DeleteRequest deleteRequest = new DeleteRequest("xizi777","emp","5");
        BulkResponse bulkItemResponses = preBuiltTransportClient.prepareBulk()
                .add(request1)
                .add(request2)
//                .add(updateRequest)
//                .add(deleteRequest)
                .get();
        BulkItemResponse[] items = bulkItemResponses.getItems();
        for (BulkItemResponse item : items) {
            System.out.println(item.status());
        }
    }





    /**
     * 分页查询
     *  From 从那条记录开始 默认从0 开始  form = (pageNow-1)*size
     *  Size 每次返回多少条符合条件的结果  默认10
     */
    @Test
    public void testMatchAllQueryFormAndSize() throws Exception {
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp")
                .setQuery(QueryBuilders.matchAllQuery()).setFrom(0).setSize(2).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits.getHits()) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




    /**
     *  查询返回指定字段(source) 默认返回所有
     *      setFetchSource 参数1:包含哪些字段   参数2:排除哪些字段
     *      setFetchSource("*","age")  返回所有字段中排除age字段
     *      setFetchSource("name","")  只返回name字段
     *      setFetchSource(new String[]{},new String[]{})
     */
    @Test
    public void testMatchAllQuerySource() throws Exception {
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp")
                .setQuery(QueryBuilders.matchAllQuery()).setFetchSource("*","age").get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




     // prefix 前缀查询
    @Test
    public void testPrefix() throws Exception {
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("name", "小");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp")
                .setQuery(prefixQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }



   // wildcardQuery 通配符查询  ？代表一个 *代表多个
    @Test
    public void testwildcardQuery() throws Exception {
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("name", "小*");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(wildcardQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




     // ids 查询
    @Test
    public void testIds() throws Exception {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("6","7");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(idsQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




     // fuzzy 查询
    @Test
    public void testFuzzy() throws Exception {
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "尹");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(fuzzyQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




    // bool 查询
    @Test
    public void testBool() throws Exception {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.should(QueryBuilders.matchAllQuery());
//        boolQueryBuilder.mustNot(QueryBuilders.rangeQuery("age").lte(8));
        boolQueryBuilder.must(QueryBuilders.termQuery("name","尹"));
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").setQuery(boolQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }




/**
 * 高亮查询
 *  .highlighter(highlightBuilder) 用来指定高亮设置           requireFieldMatch(false) 开启多个字段高亮
 *  field 用来定义高亮字段      *  preTags("<span style='color:red'>")  用来指定高亮前缀    postTags("</span>") 用来指定高亮后缀
 */
    @Test
    public void testHighlight() throws Exception {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "尹");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("name").field("content").preTags("<span style='color:red'>").postTags("</span>");
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777").setTypes("emp").highlighter(highlightBuilder).setQuery(termQueryBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println("================高亮之前==========");
            for(Map.Entry<String,Object> entry:sourceAsMap.entrySet()){
                System.out.println("key: "+entry.getKey() +"   value: "+entry.getValue());
            }
            System.out.println("================高亮之后==========");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for (Map.Entry<String,Object> entry:sourceAsMap.entrySet()){
                HighlightField highlightField = highlightFields.get(entry.getKey());
                if (highlightField!=null){
                    System.out.println("key: "+entry.getKey() +"   value: "+ highlightField.fragments()[0]);
                }else{
                    System.out.println("key: "+entry.getKey() +"   value: "+entry.getValue());
                }
            }

        }
    }

    // 过滤查询  在查询之前对大量数据进行筛选
    @Test
    public void testFilter(){
        RangeQueryBuilder age =  QueryBuilders.rangeQuery("age").gte(18).lte(21);
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777")
                .setTypes("emp")
                .setPostFilter(age)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.print(", 对应结果:=====>"+hit.getSourceAsString());
            System.out.println("=================================================");
        }
    }

    /**
     * 基于多字段关键词查询
     * 分页
     * 排序
     * 过滤
     * 执行字段返回
     * 高亮处理
     */
    @Test
    public  void All(){
        SearchResponse searchResponse = preBuiltTransportClient.prepareSearch("xizi777")
                .setTypes("emp")
                .setFrom(0)
                .setSize(20)
                .addSort("age", SortOrder.DESC) //排序
                .setSource(SearchSourceBuilder.searchSource().fetchSource("*", "sex"))  //指定字段
                .setPostFilter(QueryBuilders.termQuery("name", "伟"))
                .setQuery(QueryBuilders.multiMatchQuery("牛逼", "content","name"))  //多字段搜索
                .highlighter(new HighlightBuilder().field("*").requireFieldMatch(false).preTags("<span style='color:red'>").postTags("</span>"))
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数: "+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.print("当前索引的分数: "+hit.getScore());
            System.out.println(", 原始文档对应结果:=====>"+hit.getSourceAsString());
            System.out.println("高亮字段："+hit.getHighlightFields());
            System.out.println("=================================================");
        }
    }

    @After
    public void after(){
        preBuiltTransportClient.close();
    }

}

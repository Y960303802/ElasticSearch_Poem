package com.xizi.poem.service;


import com.xizi.poem.dao.PoemDAO;
import com.xizi.poem.elastic.repository.PoemRepository;
import com.xizi.poem.entity.Poem;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Transactional
public class PoemServiceImpl implements PoemService {

    @Autowired
    private PoemDAO poemDAO;

    @Autowired
    private PoemRepository poemRepository;


    @Autowired
    private RestHighLevelClient restHighLevelClient;


    @Override
    public List<Poem> findByKeywords(String content, String type, String author) {
        //定义list
        List<Poem> lists = null;
        try {
            SearchRequest searchRequest = new SearchRequest("poems");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //搜索条件为空指定查询条件
            if (StringUtils.isEmpty(content)) {
                //设置为查询所有
                sourceBuilder.query(QueryBuilders.matchAllQuery());
            } else {
                //设置为多字段检索
                sourceBuilder.query(QueryBuilders.multiMatchQuery(content, "name", "content", "author", "authordes"));
            }

            //指定过滤条件
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (!StringUtils.isEmpty(author)) {
               boolQueryBuilder.filter(QueryBuilders.termQuery("author",author));
            }
            if (!StringUtils.isEmpty(type)) {
                boolQueryBuilder.filter(QueryBuilders.termQuery("type",type));
            }
            //指定过滤
            sourceBuilder.postFilter(boolQueryBuilder);

            //指定高亮
            sourceBuilder.highlighter(new HighlightBuilder().field("*").requireFieldMatch(false).preTags("<span style='color:red;'>").postTags("</span>"));

            //指定显示记录
            sourceBuilder.size(100);

            //指定搜索类型
            searchRequest.types("poem").source(sourceBuilder);

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);


            //获取返回结果
            if (searchResponse.getHits().totalHits > 0) lists = new ArrayList<>();
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                Poem poem = new Poem();
                //获取原始字段
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                //获取高亮字段
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                //id
                poem.setId(hit.getId());
                //name
                poem.setName(sourceAsMap.get("name").toString());
                if (highlightFields.containsKey("name")) {
                    poem.setName(highlightFields.get("name").fragments()[0].toString());
                }
                //作者
                poem.setAuthor(sourceAsMap.get("author").toString());
                if (highlightFields.containsKey("author")) {
                    poem.setAuthor(highlightFields.get("author").fragments()[0].toString());
                }
                //作者简介
                poem.setAuthordes(sourceAsMap.get("authordes").toString());
                if (highlightFields.containsKey("authordes")) {
                    poem.setAuthordes(highlightFields.get("authordes").fragments()[0].toString());
                }
                //分类
                poem.getCategory().setName(sourceAsMap.get("category").toString());
                //内容
                poem.setContent(sourceAsMap.get("content").toString());
                if (highlightFields.containsKey("content")) {
                    poem.setContent(highlightFields.get("content").fragments()[0].toString());
                }
                //地址
                poem.setHref(sourceAsMap.get("href").toString());
                //类型
                poem.setType(sourceAsMap.get("type").toString());

                lists.add(poem);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return lists;
    }

    //清空所有文档
    @Override
    public void deleteAll() {
        //先判断ES中是否存在文档
        Iterable<Poem> all = poemRepository.findAll();
        if (all.iterator().hasNext()) {//如果存在文档删除
            poemRepository.deleteAll();
        } else {
            throw new RuntimeException("当前索引中已经没有任何文档!");
        }
    }

    //重建文档
    @Override
    public void saveAll() {
        //清空所有文档
        poemRepository.deleteAll();
        //重新创建
        //查询数据库中所有数据
        List<Poem> poems = poemDAO.findAll();
        //导入ES索引库
        poemRepository.saveAll(poems);
    }

    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public Long findTotalCounts() {
        return poemDAO.findTotalCounts();
    }

    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<Poem> findByPage(Integer page, Integer size) {
        int start = (page - 1) * size;
        return poemDAO.findByPage(start, size);
    }

}

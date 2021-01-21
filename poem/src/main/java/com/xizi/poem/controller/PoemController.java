package com.xizi.poem.controller;

import com.alibaba.druid.util.StringUtils;
import com.xizi.poem.entity.Poem;
import com.xizi.poem.service.PoemService;
import com.github.houbb.segment.support.segment.result.impl.SegmentResultHandlers;
import com.github.houbb.segment.util.SegmentHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("poem")
@Slf4j
public class PoemController {


    @Autowired
    private PoemService poemService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;


    //编辑操作
    public Map<String, Object> edit(String oper) {
        if (StringUtils.equals(oper, "add")) {

        }
        if (StringUtils.equals(oper, "edit")) {

        }
        if (StringUtils.equals(oper, "del")) {

        }
        return null;
    }


    //分页查询所有
    @RequestMapping("findByPage")
    public Map<String, Object> findByPage(Integer page, Integer rows) {
        log.info("当前页: [{}] 每页展示记录数: [{}]",page,rows);
        Map<String, Object> map = new HashMap<>();

        //查询所有诗词 每个诗中包含所属分类
        List<Poem> poems = poemService.findByPage(page, rows);
        //查询古诗总记录数
        Long totalCounts = poemService.findTotalCounts();

        //计算总页数
        Long totalPage = totalCounts % rows == 0 ? totalCounts / rows : totalCounts / rows + 1;

        map.put("page", page);
        map.put("rows", poems);
        map.put("total", totalPage);
        map.put("records", totalCounts);

        return map;
    }


    //批量录入ES索引库
    @RequestMapping("saveAll")
    public Map<String, Object> saveAll() {
        log.info("正在执行索引的创建请稍后....");
        Map<String, Object> map = new HashMap<>();
        try {
            poemService.saveAll();
            map.put("success", true);
            map.put("msg", "索引录入成功!");
        } catch (Exception e) {
            e.printStackTrace();
            map.put("success", false);
            map.put("msg", "索引录入失败:" + e.getMessage());
        }
        return map;
    }

    //清空所有文档
    @RequestMapping("deleteAll")
    public Map<String, Object> deleteAll() {
        log.info("正在执行索引的创建请稍后....");
        Map<String, Object> map = new HashMap<>();
        try {
            poemService.deleteAll();
            map.put("success", true);
            map.put("msg", "文档已全部清除!!!");
        } catch (Exception e) {
            e.printStackTrace();
            map.put("success", false);
            map.put("msg", "文档清除失败:" + e.getMessage());
        }
        return map;
    }

    //前台搜索
    //前台系统根据关键词检索
    @GetMapping("findAllKeywords")//参数1:搜索的条件   参数2:搜索类型  参数3:根据指定作者搜索
    public Map<String, Object> findAll(String content, String type, String author) {
        Map<String, Object> map = new HashMap<>();

        //放入redis
        if (!StringUtils.isEmpty(content)) {
            //对搜索进行分词处理
            List<String> segment = SegmentHelper.segment(content, SegmentResultHandlers.word());

            log.info("当前搜索分词结果为:[{}]",segment);
            segment.forEach(word->{
                if(word.length()>1){
                    stringRedisTemplate.opsForZSet().incrementScore("keywords", word, 0.5);
                }
            });
        }
        if (StringUtils.equals("所有", type)) type = null;
        if (StringUtils.equals("所有", author)) author = null;
        log.info("type:[{}] author:[{}] ", type, author);
        try {
            List<Poem> poems = poemService.findByKeywords(content, type, author);
            map.put("success", true);
            map.put("msg", "查询成功!");
            map.put("poems", poems);
        } catch (Exception e) {
            e.printStackTrace();
            map.put("success", false);
            map.put("msg", "检索出错!");

        }
        return map;
    }


    //获取redis热词排行榜
    @RequestMapping("findRedisKeywords")
    public Set<ZSetOperations.TypedTuple<String>> findRedisKeywords(){
        Set<ZSetOperations.TypedTuple<String>> keywords = stringRedisTemplate.opsForZSet().reverseRangeWithScores("keywords", 0, 20);
        return keywords;
    }


}

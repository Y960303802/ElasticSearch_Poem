package com.xizi.elasticsearch.dao;

import com.xizi.elasticsearch.pojo.Emp;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

import java.util.List;


public interface EmpRepository extends ElasticsearchRepository<Emp,String> {

    //根据姓名查询
    List<Emp> findByName(String name);

    //根据年龄查询
    List<Emp> findByAge(Integer age);

    //根据内容查询
    List<Emp> findByContent(String keyword);

    //根据内容和名字查
    List<Emp> findByNameAndContent(String name,String content);

    //根据内容或名称查询
    List<Emp> findByNameOrContent(String name,String content);

    //年龄范围查询
    List<Emp> findByAgeBetween(Integer start,Integer end);

    //年龄大于查询
    List<Emp> findByAgeGreaterThan(Integer age);

    //查询名字以xx开始的
    List<Emp>  findByNameStartingWith(String name);

    //查询名字以xx结尾的
    List<Emp>  findByNameEndingWithE(String name);

    //查询名字包含xx
    List<Emp>  findByNameContaining(String name);

    //查询某个字段值是否为false
    List<Emp>  findByNameFalse();

}

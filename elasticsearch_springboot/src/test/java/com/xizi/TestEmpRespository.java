package com.xizi;

import com.xizi.elasticsearch.dao.EmpRepository;
import com.xizi.elasticsearch.pojo.Emp;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@SpringBootTest
public class TestEmpRespository {
    @Autowired
    private EmpRepository empRepository;

    @Test
    public void testSave(){
        //保存/更新一条数据
        //id存在--更新 id不存在--添加
        Emp emp = new Emp();
        emp.setId(UUID.randomUUID().toString())
                .setName("戏子333")
                .setAge(20)
                .setBir(new Date())
                .setContent("戏子牛逼")
                .setAddress("江西南昌");
        empRepository.save(emp);
    }

    @Test
    public void delete(){
        //根据id删除一条文档
        empRepository.deleteById("f9067232-b989-444e-af23-b382d20e89a6");
    }

    @Test
    public void testDeleteAll(){
        //删除所有
        empRepository.deleteAll();
    }

    @Test
    public void testFindOne(){
        //检索一条记录
        Optional<Emp> optional = empRepository.findById("21d9e391-2190-4e45-a8bc-94e8e843b921");
        System.out.println(optional.get());
    }

    @Test
    public void testFindAll(){
        //查询所有 排序
        Iterable<Emp> all = empRepository.findAll();
        all.forEach(emp -> System.out.println(emp));
    }
    @Test
    public void testFindAllSort(){
        //查询所有 排序 升序
        Iterable<Emp> all = empRepository.findAll(Sort.by(Sort.Order.asc("age")));
        all.forEach(emp -> System.out.println(emp));
    }
    @Test
    public void testFindPage(){
        //分页
        Page<Emp> search = empRepository.search(QueryBuilders.matchAllQuery(), PageRequest.of(0, 20));
        for (Emp emp : search) {
            System.out.println(emp);
        }
    }

    @Test
    public void testFindByName() {
        // 测试自定义查询 通过name
        List<Emp> emps = empRepository.findByName("戏子333");
        for (Emp emp : emps) {
            System.out.println(emp);
        }

    }
    }

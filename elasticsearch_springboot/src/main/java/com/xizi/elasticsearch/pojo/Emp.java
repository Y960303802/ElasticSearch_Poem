package com.xizi.elasticsearch.pojo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import java.util.Date;

@Document(indexName = "emp",type = "ems")  //将Emp的对象映射成ES中一条json格式文档
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class Emp {

    //将对象中id属性与文档中_id属性对应
    @Id
    private String id;

    // 用来指定字段类型
    @Field(type = FieldType.Text,analyzer ="ik_max_word")
    private String name;

    @Field(type = FieldType.Integer)
    private Integer age;

    @Field(type = FieldType.Date)
  	@JsonFormat(pattern="yyyy-MM-dd")
    private Date bir;


    @Field(type = FieldType.Text,analyzer ="ik_max_word")
    private String content;

    @Field(type = FieldType.Text,analyzer ="ik_max_word")
    private String address;
}
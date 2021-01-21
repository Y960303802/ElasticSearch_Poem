package com.xizi.poem.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Data
@Accessors(chain = true)
public class Category {

    @Field(type= FieldType.Keyword,index = false)
    private String id;

    @Field(type = FieldType.Keyword)
    private String name;

}

//{"name":"",.....,"category":{"id":"1111","name":"七言"}}

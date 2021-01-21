package com.xizi.poem.elastic.repository;

import com.xizi.poem.entity.Poem;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface PoemRepository extends ElasticsearchRepository<Poem,String> {
}

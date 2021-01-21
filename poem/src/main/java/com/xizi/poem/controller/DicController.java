package com.xizi.poem.controller;

import org.apache.commons.io.FileUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * 实现远程词典CRUD操作
 */

@RestController
@RequestMapping("dic")
public class DicController {



    //@Autowired
    //private StringRedisTemplate stringRedisTemplate;

    //加载远程词典
    @RequestMapping(value = "remote",produces = "text/plain")
    public String remote(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String realPath = request.getServletContext().getRealPath("/");
        File file = new File(realPath, "init.dic");
        String readFileToString = FileUtils.readFileToString(file);
        response.setDateHeader("Last-Modified",System.currentTimeMillis());
        response.setHeader("ETag",DigestUtils.md5DigestAsHex(readFileToString.getBytes()));
        return readFileToString;
    }

    //获取redis热词排行榜
//    @RequestMapping("/findRedisKeywords")
//    public Set<ZSetOperations.TypedTuple<String>> findRedisKeywords(){
//        Set<ZSetOperations.TypedTuple<String>> keywords = stringRedisTemplate.opsForZSet().reverseRangeWithScores("keywords", 0, 10);
//        return keywords;
//    }

    //添加到远程词典
    @RequestMapping("save")
    public Map<String, Object> saveDic(String keyword,HttpServletRequest request){
        Map<String,Object> result = new HashMap<String,Object>();
        try {
            //对获取的数据进行去掉空格
            String trimAllWhitespace = StringUtils.trimAllWhitespace(keyword);

            //获取项目路径
            String realPath = request.getServletContext().getRealPath("/");
            //读取文件
            File file = new File(realPath, "init.dic");
            String readFileToString = FileUtils.readFileToString(file);
            if(!readFileToString.contains(trimAllWhitespace)){
                //将新的关键词写入文件
                FileUtils.write(file,trimAllWhitespace+"\r\n","UTF-8",true);
                result.put("success",true);
            }else{
                throw new RuntimeException("关键词已经存在,无须重复添加!!!");
            }
        }catch (Exception e){
            result.put("success",false);
            result.put("message",e.getMessage());
        }
        return result;
    }

    //删除词典中热词
    @RequestMapping("delete")
    public Map<String, Object> delete(String keyword, HttpServletRequest request) throws IOException {
        Map<String,Object> result = new HashMap<>();
        try{
            //获取项目绝对路径
            String realPath = request.getServletContext().getRealPath("/");
            //读取文件
            File file = new File(realPath, "init.dic");
            //使用file文件
            FileReader fileReader = new FileReader(file);
            //使用bufferread读取
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            //字符串拼接
            StringBuilder sb = new StringBuilder();
            while (true) {
                String line = bufferedReader.readLine();
                if (line == null) break;
                if (!line.equals(keyword)) {
                    sb.append(line).append("\r\n");//回车换行
                }
            }
            //将sb字符串写入指定文件
            FileUtils.write(file,sb.toString(),"UTF-8",false);
            result.put("success",true);
        }catch (Exception e){
            e.printStackTrace();
            result.put("success",false);
        }
        return result;
    }



    //获取远程词典列表
    @RequestMapping("findAll")
    public List<String> findAllDics(HttpServletRequest request) throws IOException {
        //获取当前项目名绝对路径
        String realPath = request.getServletContext().getRealPath("/");
        //读取dic文件
        File file = new File(realPath, "init.dic");
        //读取文件
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        //定义list
        List<String> list = new ArrayList<>();
        //读取文件
        while(true){
           String keywords =  bufferedReader.readLine();
           if(keywords==null)break;
           list.add(keywords);
        }
        return list;
    }
}

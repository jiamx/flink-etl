package com.etl.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/3/27
 *  @Time: 12:58
 *  
 */
public class Utility {

    /**
     * @param fileName 配置文件名称
     * @return
     */
    public static Config parseConf(String fileName) {

        Config config = ConfigFactory.load(fileName);
        return config;
    }

    //测试工具类
    /*public static void main(String[] args) {
        Config config = Utility.parseConf("types.json");
        String type1 = config.getObject("type1").toString();
        int subject_id = config.getInt("type1.subject_id");
        String name = config.getString("type1.subject_name");
        System.out.println("subject_name :" + name + " " + "subject_id :" + subject_id);

    }*/
}

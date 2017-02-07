package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;

import java.sql.SQLException;

/**
 * Created by ymhe on 2017/1/20.
 * SyncData
 */
public class syncTo78Solr {
    public static void main(String[] args){
        ARE.init();
        //初始化布隆过滤器

        //测试时注意不要修改25库标志位
        try {
            new SyncMain().sync78DataMain();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

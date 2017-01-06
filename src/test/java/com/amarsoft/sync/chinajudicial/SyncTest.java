package com.amarsoft.sync.chinajudicial;

import com.amarsoft.util.hbase.HBaseManager;

/**
 * Created by ymhe on 2017/1/4.
 * SyncData
 */
public class SyncTest {
    public static void main(String[] args){
        //测试时注意不要修改25库标志位
        new SyncMain().syncDataMain();

//        HBaseManager hBaseManager = new HBaseManager();
//        hBaseManager.init("cdh1,cdh2,cdh3","2181","courtbulletinCZ","name","PDESC");

    }
}

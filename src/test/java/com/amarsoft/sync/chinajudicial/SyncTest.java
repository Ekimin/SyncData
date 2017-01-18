package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.util.hbase.HBaseManager;

/**
 * Created by ymhe on 2017/1/4.
 * SyncData
 */
public class SyncTest {
    public static void main(String[] args){
        ARE.init();
        //初始化布隆过滤器
        BloomImpl bloomImpl = new BloomImpl();
        ARE.getLog().info("初始化布隆过滤器>>>>>>>>>>");
        bloomImpl.init();
        ARE.getLog().info("初始化布隆过滤器结束<<<<<<");
        //测试时注意不要修改25库标志位
//        new SyncMain().syncDataMain(bloomImpl);
        new SyncMain().syncYunDataMain(bloomImpl);
    }
}

package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.dao.chinajudicial.DataManager;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.bloomfilter.BloomFilterFactory;
import com.amarsoft.util.bloomfilter.BloomFilterManager;
import com.amarsoft.util.common.DateManager;
import com.amarsoft.util.hbase.HBaseManager;
import javafx.scene.effect.Bloom;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 */
public class SyncMain {

    /**
     * 中国裁判文书网数据同步主入口
     */
    public void syncDataMain(){
        ARE.init();

        while(true){
            final int BATCH_SIZE = Integer.parseInt(ARE.getProperty("BATCH_SIZE"));
            final String DATABASE = ARE.getProperty("SYNC_DATABASE");
            final String MIN_DATE = ARE.getProperty("MIN_DATE");
            final String MAX_DATE = ARE.getProperty("MAX_DATE");
            final String SOLRHOST = ARE.getProperty("SOLRHOST");

            final String ZK_HOST = ARE.getProperty("ZK_HOST");
            final String ZK_PORT = ARE.getProperty("ZK_PORT");
            final String HTABLE = ARE.getProperty("HBASE_TABLE");
            final String HFAMILY = ARE.getProperty("HBASE_FAMILY");
            final String QUALIFIER = ARE.getProperty("QUALIFIER");

            ARE.getLog().info("=======================================================");
            ARE.getLog().info("本次同步数据库：" + DATABASE);
            ARE.getLog().info("同步时间区间：" + MIN_DATE + " TO " + MAX_DATE);
            ARE.getLog().info("同步solr地址：" + SOLRHOST);
            ARE.getLog().info("同步HBase表名：" + HTABLE + "; 列簇：" + HFAMILY);
            ARE.getLog().info("BATCH_SIZE：" + BATCH_SIZE);
            ARE.getLog().info("=======================================================");

            //初始化布隆过滤器
            BloomImpl bloomImpl = new BloomImpl();
            ARE.getLog().info("初始化布隆过滤器>>>>>>>>>>");
            bloomImpl.init();
            ARE.getLog().info("初始化布隆过滤器结束<<<<<<");

            DataManager dataManager = new DataManager();
            //取数
            ARE.getLog().info("开始从数据库获取需要同步的数据>>>>>>>>");
            dataManager.getDataNumByDateRange(DATABASE, DateManager.getCurrentDate());
        }
    }


}

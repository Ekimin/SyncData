package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.dao.chinajudicial.DataManager;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.common.DateManager;
import com.amarsoft.util.hbase.HBaseManager;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 */
public class SyncMain {

    /**
     * 中国裁判文书网Aliyun数据同步主入口
     */
    public void syncYunDataMain(BloomImpl bloomImpl) throws SQLException {
        if (!ARE.isInitOk()) {
            ARE.init();
        }

        int BATCH_SIZE = Integer.parseInt(ARE.getProperty("BATCH_SIZE"));
        String DATABASE = ARE.getProperty("SYNC_DATABASE_YUN");
        String DESTDB = "destDB";
        String SOLR_HOST = ARE.getProperty("SOLR_HOST");
        int SLEEP_TIME = Integer.parseInt(ARE.getProperty("SLEEP_TIME"));
        int BLOOM_BATCH = Integer.parseInt(ARE.getProperty("BLOOM_BATCH"));
        String minDate = ARE.getProperty("MIN_DATE");
        String maxDate = ARE.getProperty("MAX_DATE");

        while (true) {

            ARE.getLog().info("=======================中国裁判文书网Aliyun数据同步开始================================");
            ARE.getLog().info("本次同步源数据库：" + DATABASE);
            ARE.getLog().info("本次同步目标数据库：" + DESTDB);
            ARE.getLog().info("本次同步时间区间：" + minDate + " TO " + maxDate);
            ARE.getLog().info("同步solr地址：" + SOLR_HOST);
            ARE.getLog().info("每一批次处理量：" + BATCH_SIZE);
            ARE.getLog().info("睡眠时间：" + SLEEP_TIME + "秒");
            ARE.getLog().info("布隆过滤器每次保存数据量：" + BLOOM_BATCH);
            ARE.getLog().info("=======================================================");

            DataManager dataManager = new DataManager();
            //取数
            ARE.getLog().info("开始从数据库获取需要同步的数据>>>>>>>>");
            int syncNum = dataManager.getYunDataNumByDateRange(DATABASE, minDate, maxDate);
            if (syncNum != 0 && syncNum != -1) {
                int totalBatch = syncNum % BATCH_SIZE == 0 ? syncNum / BATCH_SIZE : syncNum / BATCH_SIZE + 1;
                ARE.getLog().info("本次需要同步的数据量为：" + syncNum + ", 分" + totalBatch + "批进行");

                ARE.getLog().info("开始分批同步数据>>>>>>>>>>>>");

                List<DataModel> dataModelList;
                HBaseManager hBaseManager = new HBaseManager();
                SolrImpl solrImpl = new SolrImpl(SOLR_HOST);
                HBaseImpl hBaseImpl = new HBaseImpl();
                int unsavedBloom = 0;

                for (int i = 0; i < totalBatch; i++) {
                    ARE.getLog().info("开始处理第 " + (i + 1) + "/" + totalBatch + " 批*******");
                    //取数
                    dataModelList = dataManager.getYunDataByDateRange(DATABASE, minDate, maxDate, i * BATCH_SIZE, BATCH_SIZE);

                    //去重，过布隆
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始进行布隆去重>>>>>>>");
                    bloomImpl.clearDataByBloom(dataModelList);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据布隆过滤器去重完毕<<<<<<<");

                    //同步27数据库还需要正文入Hbase。
                    ARE.getLog().info("第" + (i + 1) + "批数据开始存Hbase>>>>>>>");
                    hBaseImpl.saveDataInHbase(dataModelList, hBaseManager);
                    ARE.getLog().info("第" + (i + 1) + "批数据存入Hbase完毕<<<<<<");


                    //入solr
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始同步solr>>>>>>>");
                    solrImpl.syncSolr(dataModelList, hBaseManager);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据同步solr完成<<<<<<<");

                    //入库
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始同步生产库>>>>>>>");

                    dataManager.insertYunData(dataModelList, DATABASE, DESTDB);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据同步生产库结束<<<<<<<");

                    //保存布隆过滤器
                    ARE.getLog().info("添加同步过的数据到布隆过滤器>>>>>>>");
                    bloomImpl.addBloom(dataModelList);
                    ARE.getLog().info("添加到布隆过滤器结束<<<<<");

                    unsavedBloom += BATCH_SIZE;
                    ARE.getLog().info("当前尚未保存到布隆过滤器中的数据量为：" + unsavedBloom);
                    if (unsavedBloom >= BLOOM_BATCH) {
                        ARE.getLog().info("达到布隆过滤器保存量，开始保存布隆过滤器>>>>>>>");
                        bloomImpl.saveBloom();
                        unsavedBloom = 0;
                        ARE.getLog().info("保存结束<<<<<<<<<<<<");
                    }
                    ARE.getLog().info("本批次 " + (i + 1) + "/" + totalBatch + " 同步结束*******");
                }

                ARE.getLog().info("===============保存布隆过滤器===============");
                bloomImpl.saveBloom();
                ARE.getLog().info("===============同步任务全部结束===============");

                //关闭对象
                solrImpl = null;
                hBaseManager = null;
                dataManager = null;
                dataModelList = null;
                System.gc();

                try {
                    ARE.getLog().info("===============睡眠 " + SLEEP_TIME + " 秒后进行下一轮同步===============");
                    Thread.sleep(SLEEP_TIME * 1000); //睡眠等待下一轮
                } catch (InterruptedException e) {
                    ARE.getLog().error("睡眠出错", e);
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 中国裁判文书网一级监控名单数据同步主入口
     */
    public void syncDataMain(BloomImpl bloomImpl) {
        if (!ARE.isInitOk()) {
            ARE.init();
        }

        while (true) {
            int BATCH_SIZE = Integer.parseInt(ARE.getProperty("BATCH_SIZE"));
            String DATABASE = ARE.getProperty("SYNC_DATABASE");
            String DESTDB = "destDB";
            String SOLR_HOST = ARE.getProperty("SOLR_HOST");
            int SLEEP_TIME = Integer.parseInt(ARE.getProperty("SLEEP_TIME"));
            int BLOOM_BATCH = Integer.parseInt(ARE.getProperty("BLOOM_BATCH"));

            ARE.getLog().info("=======================中国裁判文书网一级监控名单数据同步开始================================");
            ARE.getLog().info("本次同步源数据库：" + DATABASE);
            ARE.getLog().info("本次同步目标数据库：" + DESTDB);
            ARE.getLog().info("同步solr地址：" + SOLR_HOST);
            ARE.getLog().info("每一批次处理量：" + BATCH_SIZE);
            ARE.getLog().info("睡眠时间：" + SLEEP_TIME + "秒");
            ARE.getLog().info("布隆过滤器每次保存数据量：" + BLOOM_BATCH);
            ARE.getLog().info("=======================================================");


            DataManager dataManager = new DataManager();
            //取数
            String currDate = DateManager.getCurrentDate();
            ARE.getLog().info("当前系统时间：" + currDate);
            ARE.getLog().info("开始从数据库获取需要同步的数据>>>>>>>>");
            int syncNum = dataManager.getDataNumByDateRange(DATABASE, currDate);
            if (syncNum != 0 && syncNum != -1) {
                int totalBatch = syncNum % BATCH_SIZE == 0 ? syncNum / BATCH_SIZE : syncNum / BATCH_SIZE + 1;
                ARE.getLog().info("本次需要同步的数据量为：" + syncNum + ", 分" + totalBatch + "批进行");

                ARE.getLog().info("开始分批同步数据>>>>>>>>>>>>");

                List<DataModel> dataModelList;
                HBaseManager hBaseManager = new HBaseManager();
                SolrImpl solrImpl = new SolrImpl(SOLR_HOST);
                int unsavedBloom = 0;

                for (int i = 0; i < totalBatch; i++) {
                    ARE.getLog().info("开始处理第 " + (i + 1) + "/" + totalBatch + " 批*******");
                    //取数
                    dataModelList = dataManager.getDataByMaxDate(DATABASE, currDate, i, BATCH_SIZE);
                    ARE.getLog().info("获取第 " + (i + 1) + " 批数据成功!!!!!!");

                    //去重，过布隆
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始进行布隆去重>>>>>>>");
                    bloomImpl.clearDataByBloom(dataModelList);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据布隆过滤器去重完毕<<<<<<<");

                    //入solr
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始同步solr>>>>>>>");
                    solrImpl.syncSolr(dataModelList, hBaseManager);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据同步solr完成<<<<<<<");

                    //入库
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始同步生产库>>>>>>>");
                    dataManager.insertData(dataModelList, DATABASE, DESTDB);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据同步生产库结束<<<<<<<");

                    //保存布隆过滤器
                    ARE.getLog().info("添加同步过的数据到布隆过滤器>>>>>>>");
                    bloomImpl.addBloom(dataModelList);
                    ARE.getLog().info("添加到布隆过滤器结束<<<<<");


                    unsavedBloom += BATCH_SIZE;
                    ARE.getLog().info("当前尚未保存到布隆过滤器中的数据量为：" + unsavedBloom);
                    if (unsavedBloom >= BLOOM_BATCH) {
                        ARE.getLog().info("达到布隆过滤器保存量，开始保存布隆过滤器>>>>>>>");
                        bloomImpl.saveBloom();
                        unsavedBloom = 0;
                        ARE.getLog().info("保存结束<<<<<<<<<<<<");
                    }
                    ARE.getLog().info("本批次 " + (i + 1) + "/" + totalBatch + " 同步结束*******");
                }
                ARE.getLog().info("===============同步任务全部结束===============");
                bloomImpl.saveBloom();
                //关闭对象
                solrImpl = null;
                hBaseManager = null;
                dataManager = null;
                dataModelList = null;
            } else {
                ARE.getLog().info("##############本次没有查询到需要同步的数据###########");

            }


            System.gc();
            try {
                ARE.getLog().info("===============睡眠 " + SLEEP_TIME + " 秒后进行下一轮同步===============");
                Thread.sleep(SLEEP_TIME * 1000); //睡眠等待下一轮
            } catch (InterruptedException e) {
                ARE.getLog().error("睡眠出错", e);
                e.printStackTrace();
            }
        }
    }

    /**
     * 78counrtbulletion同步到solr
     */
    public void sync78DataMain() throws SQLException {
        if (!ARE.isInitOk()) {
            ARE.init();
        }

        while (true) {
            int BATCH_SIZE = Integer.parseInt(ARE.getProperty("BATCH_SIZE"));
            String DATABASE = "destDB";
            String SOLR_HOST = ARE.getProperty("SOLR_HOST");
            int SLEEP_TIME = Integer.parseInt(ARE.getProperty("SLEEP_TIME"));


            ARE.getLog().info("=======================数据同步到solr开始================================");
            ARE.getLog().info("本次同步源数据库：" + DATABASE);
            ARE.getLog().info("同步solr地址：" + SOLR_HOST);
            ARE.getLog().info("每一批次处理量：" + BATCH_SIZE);
            ARE.getLog().info("睡眠时间：" + SLEEP_TIME + "秒");
            ARE.getLog().info("=======================================================");


            DataManager dataManager = new DataManager();
            //取数
            String currDate = DateManager.getCurrentDate();
            ARE.getLog().info("开始从数据库获取需要同步的数据>>>>>>>>");
            int syncNum = dataManager.get78DataNumByDateRange(DATABASE, "2016/07/01", "2017/01/01");
            if (syncNum != 0 && syncNum != -1) {
                int totalBatch = syncNum % BATCH_SIZE == 0 ? syncNum / BATCH_SIZE : syncNum / BATCH_SIZE + 1;
                ARE.getLog().info("本次需要同步的数据量为：" + syncNum + ", 分" + totalBatch + "批进行");

                ARE.getLog().info("开始分批同步数据>>>>>>>>>>>>");

                List<DataModel> dataModelList;
                HBaseManager hBaseManager = new HBaseManager();
                SolrImpl solrImpl = new SolrImpl(SOLR_HOST);
                int unsavedBloom = 0;

                for (int i = 0; i < totalBatch; i++) {
                    ARE.getLog().info("开始处理第 " + (i + 1) + "/" + totalBatch + " 批*******");
                    //取数
                    dataModelList = dataManager.get78DataByDateRange(DATABASE, "2016/07/01", "2017/01/01", i, BATCH_SIZE);
                    ARE.getLog().info("获取第 " + (i + 1) + " 批数据成功!!!!!!");

                    //入solr
                    ARE.getLog().info("第 " + (i + 1) + " 批数据开始同步solr>>>>>>>");
                    solrImpl.syncSolr(dataModelList, hBaseManager);
                    ARE.getLog().info("第 " + (i + 1) + " 批数据同步solr完成<<<<<<<");


                    ARE.getLog().info("本批次 " + (i + 1) + "/" + totalBatch + " 同步结束*******");
                }
                ARE.getLog().info("===============同步任务全部结束===============");

                //关闭对象
                solrImpl = null;
                hBaseManager = null;
                dataManager = null;
                dataModelList = null;
            }


            System.gc();
            try {
                ARE.getLog().info("===============睡眠 " + SLEEP_TIME + " 秒后进行下一轮同步===============");
                Thread.sleep(SLEEP_TIME * 1000); //睡眠等待下一轮
            } catch (InterruptedException e) {
                ARE.getLog().error("睡眠出错", e);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ARE.init();
        //初始化布隆过滤器
        BloomImpl bloomImpl = new BloomImpl();
        ARE.getLog().info("初始化布隆过滤器>>>>>>>>>>");
        bloomImpl.init();
        ARE.getLog().info("初始化布隆过滤器结束<<<<<<");
        //测试时注意不要修改25库标志位.测试时候是同步78dbsyn.COURBULLETIN_QY到78dbdata.COURTBULLETIN
        new SyncMain().syncDataMain(bloomImpl);
//        try {
//            new SyncMain().syncYunDataMain(bloomImpl);
//        } catch (Exception e) {
//            ARE.getLog().info("同步出错", e);
//            e.printStackTrace();
//        }
    }
}

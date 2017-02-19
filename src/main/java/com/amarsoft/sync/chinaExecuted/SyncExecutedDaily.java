package com.amarsoft.sync.chinaExecuted;

import com.amarsoft.are.ARE;
import com.amarsoft.dao.chinaexecuted.ExecutedDao;
import com.amarsoft.model.chinaexecuted.ChinaExecutedModel;
import com.amarsoft.sync.common.SyncData;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.
 */

//被执行人一级监控同步
public class SyncExecutedDaily implements SyncData{

    public void syncData() {
        ExecutedDao entDao = new ExecutedDao();
        int sleepTime = Integer.valueOf(ARE.getProperty("sleepTime"));
        while(true) {
            //存储需要插入的数据
            List<ChinaExecutedModel> insertEnt = new LinkedList<ChinaExecutedModel>();
            //获得需要同步的数据
            List<ChinaExecutedModel> queryEnt = entDao.getSyncData();
            if(queryEnt.size()==0){
                ARE.getLog().info("数据库当前没有需要同步的数据，休息"+sleepTime+"秒");
                try {
                    Thread.sleep(sleepTime*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            for(ChinaExecutedModel entModel:queryEnt){
                String casecode = entModel.getCASECODE();
                String pname = entModel.getPNAME();
                String casecreatetime = entModel.getCASECREATETIME();
                List<ChinaExecutedModel> checkModels = entDao.getResultByPname(pname);
                //表示不存在该名字
                if(checkModels==null||checkModels.size()==0){
                    insertEnt.add(entModel);
                }
                else {
                    int i = 0;
                    //查找是否存在casecode和casecreateime完全一致的情况
                    for(;i<checkModels.size();i++){
                        ChinaExecutedModel chinaExecutedModel = checkModels.get(i);
                        if(casecode.equals(chinaExecutedModel.getCASECODE())&&casecreatetime.equals(chinaExecutedModel.getCASECREATETIME())){
                            break;
                        }
                    }
                    //表示存在重复的数据
                    if(i<checkModels.size()){
                        continue;
                    }
                    //表示不存在重复的数据
                    else{
                        insertEnt.add(entModel);
                    }
                }
            }
            ARE.getLog().info("开始插入数据");
            entDao.insertEntData(insertEnt);
            ARE.getLog().info("插入数据完成");
            ARE.getLog().info("更新数据表中issynchorized字段");
            entDao.updateSyncData(queryEnt);
            ARE.getLog().info("更新issynchorized完成");
        }
    }

    public static void main(String[] args) {
        ARE.init("etc/are_executed_daily.xml");
        SyncData syncData = new SyncExecutedDaily();
        syncData.syncData();
    }
}

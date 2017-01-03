package com.amarsoft.sync.lostfaith;

import com.amarsoft.are.ARE;
import com.amarsoft.dao.lostfaith.EntDao;
import com.amarsoft.model.lostfaith.EntModel;
import com.amarsoft.sync.common.SyncData;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.
 */

//失信企业一级监控同步
public class SyncEntDaily implements SyncData{

    public void syncData() {
        EntDao entDao = new EntDao();
        int sleepTime = Integer.valueOf(ARE.getProperty("sleepTime"));
        while(true) {
            //获得需要同步的数据
            //存储需要插入的数据
            List<EntModel> insertEnt = new LinkedList<EntModel>();
            //存储需要更新的数据
            List<EntModel> updateEnt = new LinkedList<EntModel>();
            List<EntModel> queryEnt = entDao.getSyncData();
            if(queryEnt.size()==0){
                ARE.getLog().info("数据库当前没有需要同步的数据，休息"+sleepTime+"秒");
                try {
                    Thread.sleep(sleepTime*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            for(EntModel entModel:queryEnt){
                String id = entModel.getId();
                String iname = entModel.getIname();
                EntModel checkModel = entDao.getResultById(id);
                //表示不存在
                if(checkModel.getId()==null){
                    insertEnt.add(entModel);
                }
                //表示该id对应的iname没有发生变化
                else if(checkModel.getIname().equals(iname)){
                    continue;
                }
                //表示iname发生变化
                else{
                    updateEnt.add(entModel);
                }
            }
            ARE.getLog().info("开始插入数据");
            entDao.insertEntData(insertEnt);
            ARE.getLog().info("插入数据完成");
            ARE.getLog().info("开始更新数据");
            entDao.updateEntDate(updateEnt);
            ARE.getLog().info("更新数据完成");
            ARE.getLog().info("更新数据表中issynchorized字段");
            entDao.updateSyncData(queryEnt);
            ARE.getLog().info("更新issynchorized完成");
        }

    }

    public static void main(String[] args) {
        ARE.init("etc/are.xml");
        SyncData syncData = new SyncEntDaily();
        syncData.syncData();
    }
}

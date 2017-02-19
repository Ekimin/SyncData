package com.amarsoft.sync.lostfaith;

import com.amarsoft.are.ARE;
import com.amarsoft.dao.lostfaith.EntDao;
import com.amarsoft.dao.lostfaith.PersonDao;
import com.amarsoft.model.lostfaith.EntModel;
import com.amarsoft.model.lostfaith.PersonModel;
import com.amarsoft.sync.common.SyncData;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.cb_lostfaith_person_daily
 */

//失信个人一级监控同步
public class SyncPersonDaily implements SyncData{

    public void syncData() {
        PersonDao entDao = new PersonDao();
        int sleepTime = Integer.valueOf(ARE.getProperty("sleepTime"));
        while(true) {
            //存储需要插入的数据
            List<PersonModel> insertEnt = new LinkedList<PersonModel>();
            //获得需要同步的数据
            List<PersonModel> queryEnt = entDao.getSyncData();
            if(queryEnt.size()==0){
                ARE.getLog().info("数据库当前没有需要同步的数据，休息"+sleepTime+"秒");
                try {
                    Thread.sleep(sleepTime*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            for(PersonModel entModel:queryEnt){
                String iname = entModel.getIname();
                String regdate = entModel.getRegdate();
                String casecode = entModel.getCasecode();
                List<PersonModel> entModelList = entDao.getResultByIname(iname);
                int i = 0 ;
                for(;i<entModelList.size();i++){
                    if(regdate.equals(entModelList.get(i).getRegdate())&&casecode.equals(entModelList.get(i).getCasecode())){
                        break;
                    }
                }
                if(i<entModelList.size()){
                    continue;
                }
                else{
                    insertEnt.add(entModel);
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
        ARE.init("etc/are_person_daily.xml");
        SyncData syncData = new SyncPersonDaily();
        syncData.syncData();
    }
}

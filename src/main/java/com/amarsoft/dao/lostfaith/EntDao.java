package com.amarsoft.dao.lostfaith;

import com.amarsoft.are.ARE;
import com.amarsoft.model.lostfaith.EntModel;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.
 */

//针对失信企业的数据库操作
public class EntDao {

    //获得需要同步的数据
    public List<EntModel> getSyncData(){
        List<EntModel> entModels = new LinkedList<EntModel>();
        int synOneTime = Integer.valueOf(ARE.getProperty("synOneTime"));



        return entModels;

    }


}

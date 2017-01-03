package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.lang.StringX;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.bloomfilter.BloomFilterManager;

import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 */
public class BloomImpl {
    /**
     * 剔除已经存在布隆过滤器中的数据
     *
     * @param dataModelList
     * @param bloomFilterManager
     */
    public void clearDataByBloom(List<DataModel> dataModelList, BloomFilterManager bloomFilterManager) {
        String url = "";

        for (DataModel dataModel : dataModelList) {
            url = dataModel.getNoticeAddress();
            if (StringX.isEmpty(url)) {
                //url为空，不同步,标记Empty
                dataModel.setURLStatus("E");
            } else {
                if (bloomFilterManager.isContain(url)) {
                    dataModel.setURLStatus("R"); //重复
                } else {
                    //不重复，需要同步
                    dataModel.setURLStatus("T");
                    //bloomFilterManager.add(url);
                }
            }
        }
        //bloomFilterManager.save();
    }
}

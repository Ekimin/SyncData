package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.are.lang.StringX;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.bloomfilter.BloomFilterFactory;
import com.amarsoft.util.bloomfilter.BloomFilterManager;

import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 * 布隆过滤器控制类实现
 */
public class BloomImpl {
    private BloomFilterManager bloomFilterManager;

    public BloomImpl(){
        //创建布隆过滤器
        bloomFilterManager = BloomFilterFactory.getDefaultBloomFiterManager();
    }

    /**
     * 初始化布隆过滤器
     */
    public void init(){
        if(bloomFilterManager == null){
            bloomFilterManager = BloomFilterFactory.getDefaultBloomFiterManager();
        }
        bloomFilterManager.init();
    }

    /**
     * 剔除已经存在布隆过滤器中的数据
     *
     * @param dataModelList
     */
    public void clearDataByBloom(List<DataModel> dataModelList) {
        if (!bloomFilterManager.isInit()){
            bloomFilterManager.init();
        }

        String url = "";

        for (DataModel dataModel : dataModelList) {
            url = dataModel.getNoticeAddress();
            if (StringX.isEmpty(url)) {
                //url为空，不同步,标记Empty
                dataModel.setURLStatus("E");
            } else {
                if (bloomFilterManager.isContain(url)) {
                    dataModel.setURLStatus("R"); //重复
                    ARE.getLog().info("URL重复：" + url); //TODO:test only
                } else {
                    //不重复，需要同步
                    dataModel.setURLStatus("T");
                }
            }
        }
        //bloomFilterManager.save();
    }

    /**
     * 保存布隆过滤器状态
     */
    public void saveBloom(){
        this.bloomFilterManager.save();
    }

    /**
     * 将同步过的数据的URL添加到布隆过滤器中
     * @param dataModelList
     */
    public void addBloom(List<DataModel> dataModelList){
        for (DataModel dataModel : dataModelList){
            String urlStatus = dataModel.getURLStatus();
            if(urlStatus != null && urlStatus.equals("R")){
                this.bloomFilterManager.add(dataModel.getNoticeAddress());
            }
        }
    }

    public BloomFilterManager getBloomFilterManager() {
        return bloomFilterManager;
    }

    public void setBloomFilterManager(BloomFilterManager bloomFilterManager) {
        this.bloomFilterManager = bloomFilterManager;
    }
}

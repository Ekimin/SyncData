package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.model.common.FileModel;
import com.amarsoft.util.hbase.HBaseManager;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 */
public class HBaseImpl {
    /**
     * 将数据（serialno,pdesc）存入到Hbase
     *
     * @param dataModelList
     * @param hBaseManager
     */
    public void saveDataInHbase(List<DataModel> dataModelList, HBaseManager hBaseManager) {

        List<FileModel> fileModelList = new LinkedList<FileModel>();
        //同步标志位为T的数据到Habse
        for (DataModel dataModel : dataModelList) {
            if (dataModel.getURLStatus() != null && dataModel.getURLStatus().equals("T")) {
                FileModel fileModel = new FileModel();
                fileModel.setSerialno(dataModel.getSerialNo());
                fileModel.setContent(dataModel.getpDesc());
                fileModelList.add(fileModel);
            }
        }


        try {
            hBaseManager.saveBatch(fileModelList);
        } catch (Exception e) {
            ARE.getLog().error("同步数据到hbase出错了", e);
            e.printStackTrace();
        } finally {
            if (fileModelList.size() > 0) {
                fileModelList.clear();
            }
        }
    }


}

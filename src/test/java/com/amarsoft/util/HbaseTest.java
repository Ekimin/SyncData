package com.amarsoft.util;

import com.amarsoft.are.ARE;
import com.amarsoft.util.hbase.HBaseManager;

/**
 * Created by ymhe on 2017/1/5.
 * SyncData
 */
public class HbaseTest {
    public static void main(String[] args){
        ARE.init();
       // HBaseManager.testHbase("MZCSZ2016120903027274");
        HBaseManager hBaseManager = new HBaseManager();
       // hBaseManager.getConnect(ARE.getProperty("HBASE_TABLE"));
        hBaseManager.getConnect("courtbulletinprd");
        hBaseManager.scanTable();

        //System.out.print(hBaseManager.getValue("2014101700001578", "PDESC"));
    }
}

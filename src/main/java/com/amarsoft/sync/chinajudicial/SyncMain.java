package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.hbase.HBaseManager;

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
     * 同步生产库
     */
    public void syncDataBase(List<DataModel> dataModelList, HBaseManager hBaseControl) {
        //建立到生产库的数据库连接
        Connection dbConn = null;
        PreparedStatement ps = null;

        String sql = "insert into COURTBULLETIN(SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT,CASEDATE,PLAINTIFF,AGENT,SECRETARY,CHIEFJUDGE,JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT,TELNO,PROVINCE,CITY,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,HTMLFILEPATH,QDATE,DOCUMENTCLASS,CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            dbConn = ARE.getDBConnection("destDB");
            ps = dbConn.prepareStatement(sql);
            dbConn.setAutoCommit(false);
            for (DataModel dataModel : dataModelList) {
                ps.setString(1, dataModel.getSerialNo());
                ps.setString(2, dataModel.getpType());
                ps.setString(3, dataModel.getCourt());
                ps.setString(4, dataModel.getParty());
                ps.setString(5, dataModel.getpDate());
                ps.setString(6, dataModel.getpDesc());
                ps.setString(7, dataModel.getDataSource());
                ps.setString(8, dataModel.getCaseNo());
                ps.setString(9, dataModel.getDepartment());
                ps.setString(10, dataModel.getCaseDate());
                ps.setString(11, dataModel.getPlaintiff());
                ps.setString(12, dataModel.getAgent());
                ps.setString(13, dataModel.getSecretary());
                ps.setString(14, dataModel.getChiefJudge());
                ps.setString(15, dataModel.getJudge());
                ps.setString(16, dataModel.getNoticeAddress());
                ps.setString(17, dataModel.getDocuClass());
                ps.setString(18, dataModel.getTarget());
                ps.setString(19, dataModel.getTargetType());
                ps.setString(20, dataModel.getTargetAmount());
                ps.setString(21, dataModel.getTelNo());
                ps.setString(22, dataModel.getProvince());
                ps.setString(23, dataModel.getCity());
                ps.setString(24, dataModel.getCaseReason());
                ps.setString(25, dataModel.getCollectionDate());
                ps.setString(26, dataModel.getDealDate());
                ps.setString(27, dataModel.getDealPerson());
                ps.setString(28, dataModel.getHTMLFilePath());
                ps.setString(29, dataModel.getqDate());
                ps.setString(30, dataModel.getDocumentClass());
                ps.setString(31, dataModel.getCaseLevel());
                ps.setString(32, dataModel.getCardNo());
                ps.setString(33, dataModel.getIPName());
                ps.setString(34, dataModel.getBatchNo());
                ps.setString(35, dataModel.getCrawlerID());

                ps.addBatch();
            }
            ps.executeBatch();
            dbConn.commit();
        } catch (SQLException e) {
            ARE.getLog().error("同步生产数据库出错", e);
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (dbConn != null) {
                    dbConn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

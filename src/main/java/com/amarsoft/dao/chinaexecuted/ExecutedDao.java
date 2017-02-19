package com.amarsoft.dao.chinaexecuted;

import com.amarsoft.are.ARE;
import com.amarsoft.model.chinaexecuted.ChinaExecutedModel;
import com.amarsoft.model.lostfaith.EntModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.
 */

//被执行人数据库操作
public class ExecutedDao {

    //获得需要同步的数据
    public List<ChinaExecutedModel> getSyncData(){
        List<ChinaExecutedModel> entModels = new LinkedList<ChinaExecutedModel>();
        int synOneTime = Integer.valueOf(ARE.getProperty("synOneTime"));
        String selectSql = "select * from cb_executed_daily where issynchorized = 0 order by spidertime desc limit 0,?";
        Connection conn1 = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn1 = ARE.getDBConnection("bdfin");
            ps = conn1.prepareStatement(selectSql);
            ps.setInt(1,synOneTime);
            rs = ps.executeQuery();
            while (rs.next()){
                ChinaExecutedModel entModel = new ChinaExecutedModel();
                entModel.setSERIALNO(rs.getString("serialno"));
                entModel.setID(rs.getString("id"));
                entModel.setPNAME(rs.getString("pname"));
                entModel.setCASECODE(rs.getString("casecode"));
                entModel.setCASESTATE(rs.getString("casestate"));
                entModel.setEXECCOURTNAME(rs.getString("EXECCOURTNAME"));
                entModel.setEXECMONEY(rs.getString("EXECMONEY"));
                entModel.setPARTYCARDNUM(rs.getString("PARTYCARDNUM"));
                entModel.setCASECREATETIME(rs.getString("CASECREATETIME"));
                entModel.setSPIDERTIME(rs.getString("SPIDERTIME"));
                entModel.setISINUSE(rs.getString("ISINUSE"));
                entModel.setINPUTTIME(rs.getString("INPUTTIME"));
                entModels.add(entModel);
            }
        } catch (SQLException e) {
            ARE.getLog().error(e);
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if(conn1!=null){
                    conn1.close();
                }
            } catch (SQLException e) {
                ARE.getLog().error(e);
            }
        }
        return entModels;

    }

    //进行插入操作
    public  void insertEntData(List<ChinaExecutedModel> entModels){
        String insertSql = "insert into cb_executed(serialno,id,pname,casecode,casestate,execcourtname,execmoney,partycardnum,casecreatetime,spidertime,isinuse,inputtime) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        if(entModels.size()==0){
            return;
        }
        Connection conn2 = null;
        PreparedStatement ps = null;

        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(insertSql);
            for(ChinaExecutedModel entModel:entModels){
                ps.setString(1,entModel.getSERIALNO());
                ps.setString(2,entModel.getID());
                ps.setString(3,entModel.getPNAME());
                ps.setString(4,entModel.getCASECODE());
                ps.setString(5,entModel.getCASESTATE());
                ps.setString(6,entModel.getEXECCOURTNAME());
                ps.setString(7,entModel.getEXECMONEY());
                ps.setString(8,entModel.getPARTYCARDNUM());
                ps.setString(9,entModel.getCASECREATETIME());
                ps.setString(10,entModel.getSPIDERTIME());
                ps.setString(11,entModel.getISINUSE());
                ps.setString(12,entModel.getINPUTTIME());
                ps.addBatch();
            }

            ps.executeBatch();
            conn2.commit();
            ps.clearBatch();
        } catch (SQLException e) {
            ARE.getLog().error(e);
        }
        finally {
            try {
                if(ps!=null) {
                    ps.close();
                }
                if(conn2!=null){
                    conn2.close();
                }
            } catch (SQLException e) {
                ARE.getLog().error(e);
            }
        }

    }

  /*  //进行更新操作
    public void updateEntDate(List<ChinaExecutedModel> entModels){
        String updateSql = "update cb_executed set pname = ?,casecode = ?,casestate=?,execcourtname=?,execmoney=?,partycardnum=?,casecreatetime=?,spidertime=?,isinuse=?,inputtime=? where id = ?";
        Connection conn2 = null;
        PreparedStatement ps = null;
        if(entModels.size()==0){
            return;
        }

        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(updateSql);
            for(ChinaExecutedModel entModel:entModels) {
                ps.setString(1, entModel.getPNAME());
                ps.setString(2, entModel.getCASECODE());
                ps.setString(3, entModel.getCASESTATE());
                ps.setString(4, entModel.getEXECCOURTNAME());
                ps.setString(5, entModel.getEXECMONEY());
                ps.setString(6, entModel.getPARTYCARDNUM());
                ps.setString(7, entModel.getCASECREATETIME());
                ps.setString(8, entModel.getSPIDERTIME());
                ps.setString(9, entModel.getISINUSE());
                ps.setString(10, entModel.getINPUTTIME());
                ps.setString(11, entModel.getID());
                ps.addBatch();
            }
            ps.executeBatch();
            conn2.commit();
            ps.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(ps!=null) {
                    ps.close();
                }
                if(conn2!=null){
                    conn2.close();
                }
            } catch (SQLException e) {
               ARE.getLog().error(e);
            }
        }

    }*/

    //根据casecode查找数据
    public List<ChinaExecutedModel> getResultByPname(String pname){
        List<ChinaExecutedModel> entList = new LinkedList<ChinaExecutedModel>();
        String checkSql = "select casecode,casecreatetime from cb_executed where pname = ?";
        Connection conn2 = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn2 = ARE.getDBConnection("dsfin");
            ps = conn2.prepareStatement(checkSql);
            ps.setString(1,pname);
            rs = ps.executeQuery();
            while(rs.next()){
                ChinaExecutedModel entModel = new ChinaExecutedModel();
                entModel.setCASECODE(rs.getString("casecode"));
                entModel.setCASECREATETIME(rs.getString("casecreatetime"));
                entList.add(entModel);
            }
        } catch (SQLException e) {
            ARE.getLog().error(e);
        }
        finally {
            try {
                if(rs!=null) {
                    rs.close();
                }
                if(ps!=null){
                    ps.close();
                }
                if(conn2!=null){
                    conn2.close();
                }
            } catch (SQLException e) {
               ARE.getLog().error(e);
            }
        }
        return entList;
    }

    public void updateSyncData(List<ChinaExecutedModel> queryEnt) {
        if(queryEnt.size()==0){
            return;
        }
        Connection conn1 = null;
        PreparedStatement ps = null;

        String updateSql = "update cb_executed_daily set issynchorized = 1 where id = ?";
        try {
            conn1 = ARE.getDBConnection("bdfin");
            conn1.setAutoCommit(false);
            ps = conn1.prepareStatement(updateSql);
            for(ChinaExecutedModel entModel : queryEnt){
                String id = entModel.getID();
                ps.setString(1,entModel.getID());
                ps.addBatch();
            }
            ps.executeBatch();
            conn1.commit();
            ps.clearBatch();

        } catch (SQLException e) {
            ARE.getLog().error(e);
        }
        finally {
            try {
                if(ps!=null) {
                    ps.close();
                }
                if(conn1!=null){
                    conn1.close();
                }
            } catch (SQLException e) {
                ARE.getLog().error(e);
            }
        }
    }
}

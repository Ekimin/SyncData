package com.amarsoft.dao.lostfaith;

import com.amarsoft.are.ARE;
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

//针对失信企业的数据库操作
public class EntDao {

    //获得需要同步的数据
    public List<EntModel> getSyncData(){
        List<EntModel> entModels = new LinkedList<EntModel>();
        int synOneTime = Integer.valueOf(ARE.getProperty("synOneTime"));
        String selectSql = "select * from cb_lostfaith_ent_daily where issynchorized = 0 order by collectiondate desc limit 0,?";
        Connection conn1 = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn1 = ARE.getDBConnection("bdfin");
            ps = conn1.prepareStatement(selectSql);
            ps.setInt(1,synOneTime);
            rs = ps.executeQuery();
            while (rs.next()){
                EntModel entModel = new EntModel();
                entModel.setId(rs.getString("id"));
                entModel.setIname(rs.getString("iname"));
                entModel.setCasecode(rs.getString("casecode"));
                entModel.setFocusnumber(rs.getString("focusnumber"));
                entModel.setCardnum(rs.getString("cardnum"));
                entModel.setBusinessentity(rs.getString("businessentity"));
                entModel.setCourtname(rs.getString("courtname"));
                entModel.setAreaname(rs.getString("areaname"));
                entModel.setPartytypename(rs.getString("partytypename"));
                entModel.setGistid(rs.getString("gistid"));
                entModel.setRegdate(rs.getString("regdate"));
                entModel.setGistunit(rs.getString("gistunit"));
                entModel.setPerformance(rs.getString("performance"));
                entModel.setDisrupttypename(rs.getString("disrupttypename"));
                entModel.setPublishdate(rs.getString("publishdate"));
                entModel.setDuty(rs.getString("duty"));
                entModel.setCollectiondate(rs.getString("collectiondate"));
                entModel.setIsinuse(rs.getString("isinuse"));
                entModel.setInputtime(rs.getString("inputtime"));
                entModel.setDutytext(rs.getString("dutytext"));
                entModels.add(entModel);
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
                e.printStackTrace();
            }
        }
        return entModels;

    }

    //进行插入操作
    public  void insertEntData(List<EntModel> entModels){
        String insertSql = "insert into cb_lostfaith_ent(id,iname,casecode,focusnumber,cardnum,businessentity,courtname,areaname,partytypename,gistid,regdate,gistunit,performance,disrupttypename,publishdate,duty,collectiondate,isinuse,inputtime,dutytext) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        if(entModels.size()==0){
            return;
        }
        Connection conn2 = null;
        PreparedStatement ps = null;
        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(insertSql);
            for(EntModel entModel:entModels){
                ps.setString(1,entModel.getId());
                ps.setString(2,entModel.getIname());
                ps.setString(3,entModel.getCasecode());
                ps.setString(4,entModel.getFocusnumber());
                ps.setString(5,entModel.getCardnum());
                ps.setString(6,entModel.getBusinessentity());
                ps.setString(7,entModel.getCourtname());
                ps.setString(8,entModel.getAreaname());
                ps.setString(9,entModel.getPartytypename());
                ps.setString(10,entModel.getGistid());
                ps.setString(11,entModel.getRegdate());
                ps.setString(12,entModel.getGistunit());
                ps.setString(13,entModel.getPerformance());
                ps.setString(14,entModel.getDisrupttypename());
                ps.setString(15,entModel.getPublishdate());
                ps.setString(16,entModel.getDuty());
                ps.setString(17,entModel.getCollectiondate());
                ps.setString(18,entModel.getIsinuse());
                ps.setString(19,entModel.getInputtime());
                ps.setString(20,entModel.getDutytext());
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
                e.printStackTrace();
            }
        }

    }

  /*  //进行更新操作
    public void updateEntDate(List<EntModel> entModels){
       *//* iname,casecode,focusnumber,cardnum,businessentity,courtname,areaname,partytypename,gistid,regdate,gistunit,performance,disrupttypename,publishdate,duty,collectiondate,isinuse,inputtime,dutytext*//*
        String updateSql = "update cb_lostfaith_ent set iname = ?,casecode = ?,focusnumber=?,cardnum=?,businessentity=?,courtname=?,areaname=?,partytypename=?,gistid=?,regdate=?,gistunit=?,performance=?,disrupttypename=?,publishdate=?,duty=?,collectiondate=?,isinuse=?,inputtime=?,dutytext=? where id = ?";

        if(entModels.size()==0){
            return;
        }
        Connection conn2 = null;
        PreparedStatement ps = null;

        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(updateSql);
            for(EntModel entModel:entModels) {
                ps.setString(1, entModel.getIname());
                ps.setString(2, entModel.getCasecode());
                ps.setString(3, entModel.getFocusnumber());
                ps.setString(4, entModel.getCardnum());
                ps.setString(5, entModel.getBusinessentity());
                ps.setString(6, entModel.getCourtname());
                ps.setString(7, entModel.getAreaname());
                ps.setString(8, entModel.getPartytypename());
                ps.setString(9, entModel.getGistid());
                ps.setString(10, entModel.getRegdate());
                ps.setString(11, entModel.getGistunit());
                ps.setString(12, entModel.getPerformance());
                ps.setString(13, entModel.getDisrupttypename());
                ps.setString(14, entModel.getPublishdate());
                ps.setString(15, entModel.getDuty());
                ps.setString(16, entModel.getCollectiondate());
                ps.setString(17, entModel.getIsinuse());
                ps.setString(18, entModel.getInputtime());
                ps.setString(19, entModel.getDutytext());
                ps.setString(20, entModel.getId());
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
                e.printStackTrace();
            }
        }

    }*/

    //根据id查找数据
    public List<EntModel> getResultByIname(String iname){
         List<EntModel> entModelList = new LinkedList<EntModel>();
         String checkSql = "select casecode,regdate from cb_lostfaith_ent where iname = ?";
         Connection conn2 = null;
         PreparedStatement ps = null;
         ResultSet rs = null;
        try {
            conn2 = ARE.getDBConnection("dsfin");
            ps = conn2.prepareStatement(checkSql);
            ps.setString(1,iname);
            rs = ps.executeQuery();
            while(rs.next()){
                EntModel entModel = new EntModel();
                entModel.setCasecode(rs.getString("casecode"));
                entModel.setRegdate(rs.getString("regdate"));
                entModelList.add(entModel);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(rs!=null) {
                    rs.close();
                }
                if(ps!=null){
                    ps.close();
                }
                if (conn2 != null) {
                    conn2.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return entModelList;
    }

    public void updateSyncData(List<EntModel> queryEnt) {

        if(queryEnt.size()==0){
            return;
        }
        Connection conn1 = null;
        PreparedStatement ps = null;

        String updateSql = "update cb_lostfaith_ent_daily set issynchorized = 1 where id = ?";
        try {
            conn1 = ARE.getDBConnection("bdfin");
            conn1.setAutoCommit(false);
            ps = conn1.prepareStatement(updateSql);
            for(EntModel entModel : queryEnt){
                String id = entModel.getId();
                ps.setString(1,entModel.getId());
                ps.addBatch();
            }
            ps.executeBatch();
            conn1.commit();
            ps.clearBatch();

        } catch (SQLException e) {
            e.printStackTrace();
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
                e.printStackTrace();
            }
        }


    }
}

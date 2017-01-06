package com.amarsoft.dao.lostfaith;

import com.amarsoft.are.ARE;
import com.amarsoft.model.lostfaith.EntModel;
import com.amarsoft.model.lostfaith.PersonModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ryang on 2017/1/3.
 */

//失信个人数据库操作
public class PersonDao {
    //获得需要同步的数据
    public List<PersonModel> getSyncData(){
        Connection conn1 = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<PersonModel> entModels = new LinkedList<PersonModel>();
        int synOneTime = Integer.valueOf(ARE.getProperty("synOneTime"));
        String selectSql = "select * from cb_lostfaith_person_daily where issynchorized = 0 order by collectiondate desc limit 0,?";

        try {
            conn1 = ARE.getDBConnection("bdfin");
            ps = conn1.prepareStatement(selectSql);
            ps.setInt(1,synOneTime);
            rs = ps.executeQuery();
            while (rs.next()){
                PersonModel entModel = new PersonModel();
                entModel.setId(rs.getString("id"));
                entModel.setIname(rs.getString("iname"));
                entModel.setCasecode(rs.getString("casecode"));
                entModel.setAge(rs.getString("age"));
                entModel.setSexy(rs.getString("sexy"));
                entModel.setFocusnumber(rs.getString("focusnumber"));
                entModel.setCardnum(rs.getString("cardnum"));
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
    public  void insertEntData(List<PersonModel> entModels){
        Connection conn2 = null;
        PreparedStatement ps = null;
        String insertSql = "insert into cb_lostfaith_person(id,iname,casecode,age,sexy,focusnumber,cardnum,courtname,areaname,partytypename,gistid,regdate,gistunit,performance,disrupttypename,publishdate,duty,collectiondate,isinuse,inputtime,dutytext) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        if(entModels.size()==0){
            return;
        }

        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(insertSql);
            for(PersonModel entModel:entModels){
                ps.setString(1,entModel.getId());
                ps.setString(2,entModel.getIname());
                ps.setString(3,entModel.getCasecode());
                ps.setString(4,entModel.getAge());
                ps.setString(5,entModel.getSexy());
                ps.setString(6,entModel.getFocusnumber());
                ps.setString(7,entModel.getCardnum());
                ps.setString(8,entModel.getCourtname());
                ps.setString(9,entModel.getAreaname());
                ps.setString(10,entModel.getPartytypename());
                ps.setString(11,entModel.getGistid());
                ps.setString(12,entModel.getRegdate());
                ps.setString(13,entModel.getGistunit());
                ps.setString(14,entModel.getPerformance());
                ps.setString(15,entModel.getDisrupttypename());
                ps.setString(16,entModel.getPublishdate());
                ps.setString(17,entModel.getDuty());
                ps.setString(18,entModel.getCollectiondate());
                ps.setString(19,entModel.getIsinuse());
                ps.setString(20,entModel.getInputtime());
                ps.setString(21,entModel.getDutytext());
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

    //进行更新操作
    public void updateEntDate(List<PersonModel> entModels){
       /* id,iname,casecode,age,sexy,focusnumber,cardnum,courtname,areaname,patrytypename,gistid,regdate,gistunit,performance,disrupttypename,publishdate,duty,collectiondate,isinuse,inputtime,dutytext*/
         Connection conn2 = null;
         PreparedStatement ps = null;

       String updateSql = "update cb_lostfaith_person set iname = ?,casecode = ?,age=?,sexy=?,focusnumber=?,cardnum=?,courtname=?,areaname=?,partytypename=?,gistid=?,regdate=?,gistunit=?,performance=?,disrupttypename=?,publishdate=?,duty = ?,collectiondate=?,isinuse=?,inputtime=?,dutytext=? where id = ?";

        if(entModels.size()==0){
            return;
        }

        try {
            conn2 = ARE.getDBConnection("dsfin");
            conn2.setAutoCommit(false);
            ps = conn2.prepareStatement(updateSql);
            for(PersonModel entModel:entModels) {
                ps.setString(1,entModel.getIname());
                ps.setString(2,entModel.getCasecode());
                ps.setString(3,entModel.getAge());
                ps.setString(4,entModel.getSexy());
                ps.setString(5,entModel.getFocusnumber());
                ps.setString(6,entModel.getCardnum());
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
                ps.setString(21,entModel.getId());
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

    //根据id查找数据
    public PersonModel getResultById(String id){
        PersonModel entModel = new PersonModel();
        Connection conn2 = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String checkSql = "select id,iname from cb_lostfaith_person where id = ?";

        try {
            conn2 = ARE.getDBConnection("dsfin");
            ps = conn2.prepareStatement(checkSql);
            ps.setString(1,id);
            rs = ps.executeQuery();
            if(rs.next()){
                entModel.setId(id);
                entModel.setIname(rs.getString("iname"));
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
                if(conn2!=null){
                    conn2.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return entModel;
    }

    public void updateSyncData(List<PersonModel> queryEnt) {
        Connection conn1 = null;
        PreparedStatement ps = null;

        if(queryEnt.size()==0){
            return;
        }

        String updateSql = "update cb_lostfaith_person_daily set issynchorized = 1 where id = ?";
        try {
            conn1 = ARE.getDBConnection("bdfin");
            conn1.setAutoCommit(false);
            ps = conn1.prepareStatement(updateSql);
            for(PersonModel entModel : queryEnt){
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

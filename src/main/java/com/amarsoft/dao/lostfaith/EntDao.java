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
    static Connection conn1 = null;
    static Connection conn2 = null;
    static PreparedStatement ps = null;
    static ResultSet rs = null;
    static {
        //数据库连接的初始化
        try {
            if(conn1 == null) {
                conn1 = ARE.getDBConnection("bdfin");
            }
            if(conn2 == null){
                conn2 = ARE.getDBConnection("dsfin");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //获得需要同步的数据
    public List<EntModel> getSyncData(){
        List<EntModel> entModels = new LinkedList<EntModel>();
        int synOneTime = Integer.valueOf(ARE.getProperty("synOneTime"));
        String selectSql = "select * from cb_lostfaith_ent where synOneTime = 0 order by collectiondate desc limit 0,?";

        try {
            ps = conn1.prepareStatement(selectSql);
            ps.setInt(1,synOneTime);
            rs = ps.executeQuery();
            while (rs.next()){
                EntModel entModel = new EntModel();

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entModels;

    }

    public  void insertEntData(List<EntModel> entModels){

    }

    public void updateEntDate(List<EntModel> entModels){

    }

    public EntModel getResultById(String id){
         EntModel entModel = new EntModel();

         return entModel;
    }

}

package com.amarsoft.dao.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.are.lang.StringX;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.bloomfilter.BloomFilterManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ymhe on 2016/12/27.
 * ChinaJudicialBloomMerge
 */
public class DataManager {

    /**
     * 按时间区间获取指定数据库中数据的数量
     *
     * @param dataBase 数据库标识
     * @param minDate  时间区间下限
     * @param maxDate  时间区间上限
     * @return 数据量
     */
    public int getDataNumByDateRange(String dataBase, String minDate, String maxDate) {
        String sql = "SELECT COUNT(1) FROM COURTBULLETIN WHERE COLLECTIONDATE > '" + minDate +
                "' AND COLLECTIONDATE < '" + maxDate + "' AND ISSYNCHED = 'N'";
        ARE.getLog().info("SearchSQL=" + sql);
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ARE.getDBConnection(dataBase);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            if (rs.next()) {
                int dataNum = rs.getInt(1);
                return dataNum;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return -1;
    }

    /**
     * 分批获取源表数据
     *
     * @param database  数据库标识
     * @param minDate   时间区间下限
     * @param maxDate   时间区间上限
     * @param start     游标开始
     * @param batchSize 每一批的数量
     * @return 该批次数据列表
     */
    public List<DataModel> getDataByDateRange(String database, String minDate, String maxDate, int start, int batchSize) {
        String sql = "select SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT,CASEDATE,PLAINTIFF," +
                "AGENT,SECRETARY,CHIEFJUDGE,JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT,TELNO,PROVINCE," +
                "CITY,FILEPATH,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,COURTROOM,HTMLFILEPATH,QDATE," +
                "DOCUMENTCLASS,CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID,ISSYNCHED from COURTBULLETIN where COLLECTIONDATE > ? " +
                "and COLLECTIONDATE<? and ISSYNCHED = 'N' and DATASOURCE='中国裁判文书网' order by collectiondate " +
                "limit ?,?";

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<DataModel> dataModelList = new LinkedList<DataModel>();
        try {
            conn = ARE.getDBConnection(database);
            ps = conn.prepareStatement(sql);
            ps.setString(1, minDate);
            ps.setString(2, maxDate);
            ps.setInt(3, start);
            ps.setInt(4, batchSize);
            rs = ps.executeQuery();

            String serialNoYun = "";
//            同步27数据需要在serialno中加入字符以区别
//            if (database != null && database.equals("mysql_27")) {
//                serialNoYun = "CLOUD";
//            }

            //批量加入ListModel
            while (rs.next()) {
                DataModel dataModel = new DataModel();
                String newSerialNo = rs.getString("SERIALNO");
                newSerialNo = newSerialNo.substring(0, 4) + serialNoYun + newSerialNo.substring(4, newSerialNo.length());
                //ARE.getLog().info("test:serialyunno====>" + newSerialNo);
                dataModel.setSerialNo(newSerialNo);
                dataModel.setpType(rs.getString("PTYPE"));
                dataModel.setCourt(rs.getString("COURT"));
                dataModel.setParty(rs.getString("PARTY"));
                dataModel.setpDate(rs.getString("PDATE"));
                dataModel.setpDesc(rs.getString("PDESC"));
                dataModel.setDataSource(rs.getString("DATASOURCE"));
                dataModel.setCaseNo(rs.getString("CASENO"));
                dataModel.setDepartment(rs.getString("DEPARTMENT"));
                dataModel.setCaseDate(rs.getString("CASEDATE"));
                dataModel.setPlaintiff(rs.getString("PLAINTIFF"));
                dataModel.setAgent(rs.getString("AGENT"));
                dataModel.setSecretary(rs.getString("SECRETARY"));
                dataModel.setChiefJudge(rs.getString("CHIEFJUDGE"));
                dataModel.setJudge(rs.getString("JUDGE"));
                dataModel.setNoticeAddress(rs.getString("NOTICEADDR"));
                dataModel.setDocumentClass(rs.getString("DOCUCLASS"));
                dataModel.setTarget(rs.getString("TARGET"));
                dataModel.setTargetType(rs.getString("TARGETTYPE"));
                dataModel.setTargetAmount(rs.getString("TARGETAMOUNT"));
                dataModel.setTelNo(rs.getString("TELNO"));
                dataModel.setProvince(rs.getString("PROVINCE"));
                dataModel.setCity(rs.getString("CITY"));
                dataModel.setFilePath(rs.getString("FILEPATH"));
                dataModel.setCaseReason(rs.getString("CASEREASON"));
                dataModel.setCollectionDate(rs.getString("COLLECTIONDATE"));
                dataModel.setDealDate(rs.getString("DEALDATE"));
                dataModel.setDealPerson(rs.getString("DEALPERSON"));
                dataModel.setCourtRoom(rs.getString("COURTROOM"));
                dataModel.setHTMLFilePath(rs.getString("HTMLFILEPATH"));
                dataModel.setqDate(rs.getString("QDATE"));
                dataModel.setDocumentClass(rs.getString("DOCUMENTCLASS"));
                dataModel.setCaseLevel(rs.getString("CASELEVEL"));
                dataModel.setCardNo(rs.getString("CARDNO"));
                dataModel.setIPName(rs.getString("IPNAME"));
                dataModel.setBatchNo(rs.getString("BATCHNO"));
                dataModel.setCrawlerID(rs.getString("CRAWLERID"));
                dataModel.setIsSynched(rs.getString("ISSYNCHED"));//是否已经同步了:Y-已经同步过了，N-尚未
                dataModelList.add(dataModel);
            }
        } catch (SQLException e) {
            ARE.getLog().error("从数据库获取数据出错", e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return dataModelList;
    }








}

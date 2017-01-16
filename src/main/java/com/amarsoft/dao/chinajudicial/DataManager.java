package com.amarsoft.dao.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.are.lang.StringX;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.bloomfilter.BloomFilterManager;
import com.amarsoft.util.hbase.HBaseManager;
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
 */
public class DataManager {

    /**
     * 按同步截止时间获取指定数据库中数据的数量
     * <li>表名COURTBULLETIN_QY</li>
     *
     * @param dataBase
     * @param maxDate
     * @return 数据量，出错返回 -1
     */
    public int getDataNumByDateRange(String dataBase, String maxDate) {
        String sql = "SELECT COUNT(1) FROM COURTBULLETIN_QY WHERE COLLECTIONDATE < '" + maxDate + "' AND STATUS = 'waiting'";
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
     * 按时间区间获取指定数据库中数据的数量
     *
     * @param dataBase 数据库标识
     * @param minDate  时间区间下限
     * @param maxDate  时间区间上限
     * @return 数据量
     */
    public int getDataNumByDateRange(String dataBase, String minDate, String maxDate) {
        String sql = "SELECT COUNT(1) FROM COURTBULLETIN_QY WHERE COLLECTIONDATE > '" + minDate +
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
     * 按截止时间获取指定数据库中需要同步的数据
     * <li>FOR 25数据库同步中国裁判文书网数据</li>
     *
     * @param database
     * @param maxDate
     * @param start
     * @param batchSize
     * @return
     */
    public List<DataModel> getDataByMaxDate(String database, String maxDate, int start, int batchSize) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<DataModel> dataModelList = new LinkedList<DataModel>();
        String status = "waiting"; //这个是标志等待同步的字段

        String sql = "select SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT,CASEDATE,PLAINTIFF,AGENT,SECRETARY,CHIEFJUDGE"
                + ",JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT,TELNO,PROVINCE,CITY,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,HTMLFILEPATH,"
                + "QDATE,DOCUMENTCLASS,CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID from COURTBULLETIN_QY where collectiondate < ? and status='" + status + "' limit ?,?"; //35items

        try {
            conn = ARE.getDBConnection(database);
            ps = conn.prepareStatement(sql);
            ps.setString(1, maxDate);
            ps.setInt(2, start * batchSize);
            ps.setInt(3, batchSize);
            rs = ps.executeQuery();

            //序列号中加入特殊字符用于和其他途径同步的数据进行区别
            String addedSerialNo = "";
//            addedSerialNo = "LOC"; //25库已经在序列号前面加上了M
            while (rs.next()) {
                DataModel dataModel = new DataModel();
                String newSerialNo = rs.getString("SERIALNO");
//                newSerialNo = newSerialNo.substring(0, 4) + addedSerialNo + newSerialNo.substring(4, newSerialNo.length());
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
                dataModel.setCaseReason(rs.getString("CASEREASON"));
                dataModel.setCollectionDate(rs.getString("COLLECTIONDATE"));
                dataModel.setDealDate(rs.getString("DEALDATE"));
                dataModel.setDealPerson(rs.getString("DEALPERSON"));
                dataModel.setHTMLFilePath(rs.getString("HTMLFILEPATH"));
                dataModel.setqDate(rs.getString("QDATE"));
                dataModel.setDocumentClass(rs.getString("DOCUMENTCLASS"));
                dataModel.setCaseLevel(rs.getString("CASELEVEL"));
                dataModel.setCardNo(rs.getString("CARDNO"));
                dataModel.setIPName(rs.getString("IPNAME"));
                dataModel.setBatchNo(rs.getString("BATCHNO"));
                dataModel.setCrawlerID(rs.getString("CRAWLERID"));

                dataModel.setStatus(status);
                dataModelList.add(dataModel);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if(rs != null){
                    rs.close();
                }
                if(ps != null){
                    ps.close();
                }
                if(conn != null){
                    conn.close();
                }
            }catch(SQLException e){
                e.printStackTrace();
            }
        }


        return dataModelList;
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
    public List<DataModel> getDataByMaxDate(String database, String minDate, String maxDate, int start, int batchSize) {
        String sql = "select SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT,CASEDATE,PLAINTIFF," +
                "AGENT,SECRETARY,CHIEFJUDGE,JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT,TELNO,PROVINCE," +
                "CITY,FILEPATH,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,COURTROOM,HTMLFILEPATH,QDATE," +
                "DOCUMENTCLASS,CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID,ISSYNCHED from COURTBULLETIN_QY where COLLECTIONDATE > ? " +
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
            //同步27数据需要在serialno中加入字符以区别
            if (database != null && database.equals("mysql_27")) {
                serialNoYun = "CLOUD";
            }

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

    /**
     * 向数据库中插入数据,并更新标志位
     * <li>目前25库正文存的是路径，同步到生产时候也存路径（直接同步pdesc字段）</li>
     *
     * @param dataModelList 数据list
     * @param sourceDB 源数据库
     * @param destDB 目标数据库
     */
    public void insertData(List<DataModel> dataModelList, String sourceDB, String destDB) {
        Connection conn_Dest = null;
        Connection conn_Sour = null;
        PreparedStatement ps_Insert = null;
        PreparedStatement ps_Status = null;

        String sql_Insert = "insert into COURTBULLETIN_QY(SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT," +
                "CASEDATE,PLAINTIFF,AGENT,SECRETARY,CHIEFJUDGE,JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT," +
                "TELNO,PROVINCE,CITY,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,HTMLFILEPATH,QDATE,DOCUMENTCLASS," +
                "CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        String sql_Status = "UPDATE COURTBULLETIN_QY SET STATUS = ? WHERE SERIALNO = ?";
        String status1 = "invalid";
        String status2 = "success";


        try {
            conn_Sour = ARE.getDBConnection(sourceDB);
            conn_Dest = ARE.getDBConnection(destDB);
            ps_Insert = conn_Dest.prepareStatement(sql_Insert);
            ps_Status = conn_Sour.prepareStatement(sql_Status);
            conn_Dest.setAutoCommit(false);
            conn_Sour.setAutoCommit(false);
            int updateBatch = 500; //500一批提交更新
            int currCount = 0;
            for (DataModel dataModel : dataModelList) {
                //被布隆过滤的数据不入库
                if (!StringX.isEmpty(dataModel.getURLStatus()) && dataModel.getURLStatus().equals("R")) {
                    ps_Status.setString(1, status1);
                    ps_Status.setString(2, dataModel.getSerialNo());
                    ps_Status.addBatch();
                    currCount++;
                    continue;
                }

                ps_Insert.setString(1, dataModel.getSerialNo());
                ps_Insert.setString(2, dataModel.getpType());
                ps_Insert.setString(3, dataModel.getCourt());
                ps_Insert.setString(4, dataModel.getParty());
                ps_Insert.setString(5, dataModel.getpDate());
                ps_Insert.setString(6, dataModel.getpDesc());
                ps_Insert.setString(7, dataModel.getDataSource());
                ps_Insert.setString(8, dataModel.getCaseNo());
                ps_Insert.setString(9, dataModel.getDepartment());
                ps_Insert.setString(10, dataModel.getCaseDate());
                ps_Insert.setString(11, dataModel.getPlaintiff());
                ps_Insert.setString(12, dataModel.getAgent());
                ps_Insert.setString(13, dataModel.getSecretary());
                ps_Insert.setString(14, dataModel.getChiefJudge());
                ps_Insert.setString(15, dataModel.getJudge());
                ps_Insert.setString(16, dataModel.getNoticeAddress());
                ps_Insert.setString(17, dataModel.getDocuClass());
                ps_Insert.setString(18, dataModel.getTarget());
                ps_Insert.setString(19, dataModel.getTargetType());
                ps_Insert.setString(20, dataModel.getTargetAmount());
                ps_Insert.setString(21, dataModel.getTelNo());
                ps_Insert.setString(22, dataModel.getProvince());
                ps_Insert.setString(23, dataModel.getCity());
                ps_Insert.setString(24, dataModel.getCaseReason());
                ps_Insert.setString(25, dataModel.getCollectionDate());
                ps_Insert.setString(26, dataModel.getDealDate());
                ps_Insert.setString(27, dataModel.getDealPerson());
                ps_Insert.setString(28, dataModel.getHTMLFilePath());
                ps_Insert.setString(29, dataModel.getqDate());
                ps_Insert.setString(30, dataModel.getDocumentClass());
                ps_Insert.setString(31, dataModel.getCaseLevel());
                ps_Insert.setString(32, dataModel.getCardNo());
                ps_Insert.setString(33, dataModel.getIPName());
                ps_Insert.setString(34, dataModel.getBatchNo());
                ps_Insert.setString(35, dataModel.getCrawlerID());

                ps_Insert.addBatch();
                ps_Status.setString(1, status2);
                ps_Status.setString(2, dataModel.getSerialNo());
                ps_Status.addBatch();
                currCount++;
                if (currCount >= updateBatch) {
                    ps_Insert.executeBatch();
                    currCount = 0;
                }
            }
            ps_Insert.executeBatch();
            ps_Status.executeBatch();
            conn_Dest.commit();
            conn_Sour.commit();
        } catch (SQLException e) {
            ARE.getLog().error("同步生产数据库出错", e);
            e.printStackTrace();
        } finally {
            try {
                if (ps_Insert != null) {
                    ps_Insert.close();
                }
                if (ps_Status != null) {
                    ps_Status.close();
                }
                if (conn_Dest != null) {
                    conn_Dest.close();
                }
                if (conn_Sour != null){
                    conn_Sour.close();
                }
            } catch (SQLException e) {
                ARE.getLog().error("同步生产数据库时关闭数据库连接出错", e);
                e.printStackTrace();
            }
        }
    }
}

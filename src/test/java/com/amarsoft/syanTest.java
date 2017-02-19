package com.amarsoft;

import com.amarsoft.are.ARE;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.sync.chinajudicial.SolrImpl;
import com.amarsoft.util.hbase.HBaseManager;
import com.amarsoft.util.solr.SolrManager;
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
 * Created by ymhe on 2017/1/20.
 * SyncData
 */
public class syanTest {
    private SolrManager solrManager;

    public syanTest(String solrHost) {
        this.solrManager = new SolrManager(solrHost);
    }

    public static void main(String[] args) {
        ARE.init();

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        syanTest syanTest = new syanTest(ARE.getProperty("SOLR_HOST"));
        List<DataModel> dataModelList;
        try {
            int syncNum = syanTest.getYunDataNumByDateRange("bdsyn", "2016/07/01", "2016/08/01");
            conn = ARE.getDBConnection("bdsyn");
            SolrImpl solrImpl = new SolrImpl(ARE.getProperty("SOLR_HOST"));

            if (syncNum != 0 && syncNum != -1) {
                int totalBatch = syncNum % 1000 == 0 ? syncNum / 1000 : syncNum / 1000 + 1;
                ARE.getLog().info("本次需要同步的数据量为：" + syncNum + ", 分" + totalBatch + "批进行");
                for (int i = 0; i < totalBatch; i++) {
                    ARE.getLog().info("开始处理第 " + (i + 1) + "/" + totalBatch + " 批*******");
                    //取数
                    dataModelList = syanTest.getYunDataByDateRange("bdsyn", "2016/07/01", "2016/08/01", i * 1000, 1000);
                    syanTest.syncSolr(dataModelList);
                }
                ARE.getLog().info("开始分批同步数据>>>>>>>>>>>>");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public int getYunDataNumByDateRange(String dataBase, String minDate, String maxDate) throws SQLException {
        String sql = "SELECT COUNT(1) FROM COURTBULLETIN WHERE COLLECTIONDATE > '" + minDate +
                "' AND COLLECTIONDATE < '" + maxDate +"'";
        ARE.getLog().info("SearchSQL=" + sql);
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = ARE.getDBConnection(dataBase);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
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

    public List<DataModel> getYunDataByDateRange(String database, String minDate, String maxDate, int start, int batchSize) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<DataModel> dataModelList = new LinkedList<DataModel>();
//        String status = "waiting"; //这个是标志等待同步的字段

        String sql = "select SERIALNO,PTYPE,COURT,PARTY,PDATE,PDESC,DATASOURCE,CASENO,DEPARTMENT,CASEDATE,PLAINTIFF," +
                "AGENT,SECRETARY,CHIEFJUDGE,JUDGE,NOTICEADDR,DOCUCLASS,TARGET,TARGETTYPE,TARGETAMOUNT,TELNO,PROVINCE," +
                "CITY,FILEPATH,CASEREASON,COLLECTIONDATE,DEALDATE,DEALPERSON,COURTROOM,HTMLFILEPATH,QDATE," +
                "DOCUMENTCLASS,CASELEVEL,CARDNO,IPNAME,BATCHNO,CRAWLERID from COURTBULLETIN where COLLECTIONDATE > ? " +
                "and COLLECTIONDATE<? order by collectiondate " +
                "limit ?,?";
        ARE.getLog().info("开始从数据库获取数据>>>>>>>>");
        try {
            conn = ARE.getDBConnection(database);
            ps = conn.prepareStatement(sql);
            ps.setString(1, minDate);
            ps.setString(2, maxDate);
            ps.setInt(3, start);
            ps.setInt(4, batchSize);
            rs = ps.executeQuery();

            String serialNoYun = "";
            //serialNoYun = "CLOUD";

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
                dataModelList.add(dataModel);
            }
            ARE.getLog().info("从数据库获取数据完成<<<<<<<<");

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

    public void syncSolr(List<DataModel> dataModelList) {
        List<SolrInputDocument> solrInputDocumentList = new LinkedList<SolrInputDocument>();
        SolrInputDocument document;


        for (DataModel dataModel : dataModelList) {
            //跳过重复和空URL的数据
            if (dataModel.getURLStatus() != null && !dataModel.getURLStatus().equals("T")) {
                continue;
            }

            document = new SolrInputDocument();

            document.addField("serialno", dataModel.getSerialNo());
            document.addField("ptype", dataModel.getpType());
            document.addField("court", dataModel.getCourt());
            document.addField("party", dataModel.getParty());
            document.addField("pdate", dataModel.getpDate());
            document.addField("datasource", dataModel.getDataSource());
            document.addField("caseno", dataModel.getCaseNo());
            document.addField("department", dataModel.getDepartment());
            document.addField("casedate", dataModel.getCaseDate());
            document.addField("plaintiff", dataModel.getPlaintiff());
            document.addField("agent", dataModel.getAgent());
            document.addField("secretary", dataModel.getSecretary());
            document.addField("chiefjudge", dataModel.getChiefJudge());
            document.addField("judge", dataModel.getJudge());
            document.addField("noticeaddr", dataModel.getNoticeAddress());
            document.addField("docuclass", dataModel.getDocuClass());
            document.addField("target", dataModel.getTarget());
            document.addField("targettype", dataModel.getTargetType());
            document.addField("targetamount", dataModel.getTargetAmount());
            document.addField("telno", dataModel.getTelNo());
            document.addField("province", dataModel.getProvince());
            document.addField("city", dataModel.getCity());
            document.addField("casereason", dataModel.getCaseReason());
            document.addField("collectiondate", dataModel.getCollectionDate());
            document.addField("dealdate", dataModel.getDealDate());
            document.addField("dealperson", dataModel.getDealPerson());
            document.addField("filepath", dataModel.getFilePath());

            document.addField("pdesc", dataModel.getpDesc());
            System.out.println(dataModel.getSerialNo() + "++++++" + dataModel.getpDesc());
            solrInputDocumentList.add(document);
        }


        try {
            //提交同步请求
            if(solrInputDocumentList.size()>0){
                this.solrManager.getServer().add(solrInputDocumentList);
                this.solrManager.getServer().commit();
            }
        } catch (SolrServerException e) {
            ARE.getLog().error("同步solr时出现错误", e);
            e.printStackTrace();
        } catch (IOException e) {
            ARE.getLog().error("同步solr时出现错误", e);
            e.printStackTrace();
        } finally {
            if (solrInputDocumentList.size() > 0) {
                solrInputDocumentList.clear();
            }
        }
    }
}

package com.amarsoft.sync.chinajudicial;

import com.amarsoft.are.ARE;
import com.amarsoft.model.chinajudicial.DataModel;
import com.amarsoft.util.hbase.HBaseManager;
import com.amarsoft.util.solr.SolrManager;
import org.apache.solr.client.solrj.SolrServerException;

import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ymhe on 2017/1/3.
 * SyncData
 */
public class SolrImpl {
    private SolrManager solrManager;

    public SolrImpl(String solrHost) {
        this.solrManager = new SolrManager(solrHost);
    }

    /**
     * 同步solr
     * <li>文章正文数据来自Hbase</li>
     *
     * @param dataModelList
     * @param hBaseManager
     */
    public void syncSolr(List<DataModel> dataModelList, HBaseManager hBaseManager) {
        List<SolrInputDocument> solrInputDocumentList = new LinkedList<SolrInputDocument>();
        SolrInputDocument document;
        //建立Hbase连接
        hBaseManager.getConnect(ARE.getProperty("HBASE_TABLE"));

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
            //正文从Hbase中取
            String text = "";
            try {
                text = hBaseManager.getValue(dataModel.getSerialNo(), HBaseManager.getQUALIFIER());
                //ARE.getLog().info("testonly text=" + text);
            } catch (Exception e) {
                ARE.getLog().error("从Hbase中取数据出错", e);
                e.printStackTrace();
            }
            document.addField("pdesc", text);
            solrInputDocumentList.add(document);
        }
        //关闭hbase连接
        hBaseManager.connClose();

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

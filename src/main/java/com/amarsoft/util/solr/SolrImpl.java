package com.amarsoft.util.solr;

import com.amarsoft.are.ARE;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * author BY james@NJU 
 * date:2014-05-12
 */
public class SolrImpl {
	private SolrServer server;

	public SolrImpl(String indexHost) {
		try{
			this.server = new HttpSolrServer(indexHost);
		}catch (Exception e){
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) throws Exception{
		SolrImpl solr=new SolrImpl("http://192.168.61.241:8070/solrlira");
		List<String> serialNOs=new ArrayList<String>();
		serialNOs.add("SZHB20160803969645000446005");
		serialNOs.add("SZHB20160803712817000445996");
		serialNOs.add("SZHB20160803391066000445927");
		serialNOs.add("SZHB20160803714875000446190");
		serialNOs.add("SZHB20160803439512000445832");
		serialNOs.add("SZHB20160803576476000446159");
		solr.deleteByID(serialNOs);
//		solr.deleteAllIndex();
//		System.out.println("删除完成!");
		/*Connection conn = DBConnection.getConnect();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "SELECT SERIALNO FROM COURTBULLETIN where serialno not like 'SZHB%' limit 0,20000";
		List<String> serialNOs=new ArrayList<String>();
		ps=conn.prepareStatement(sql);
		rs=ps.executeQuery();
		int i=0;
		while(rs.next()){
			serialNOs.add(rs.getString("SERIALNO"));
		}
		solr.deleteByID(serialNOs);*/
//		solr.deleteAllIndex();
//		System.out.println(a);
	}
	
	/**
	 * 根据流水号来查询solr是否有该记录
	 * @return
	 */
	public boolean  queryByID(String serialNO){
		SolrQuery params=new SolrQuery();
		params.set("q", "serialno:"+serialNO);
		QueryResponse response=null;
		try {
			response = server.query(params);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		SolrDocumentList list=response.getResults();
		if(list==null||list.size()==0||list.get(0)==null){
			ARE.getLog().info("solr无："+serialNO);
			return false;
		}
		return true;
	}
	
	/**
	 * 获取最新的索引CollectionDate时间
	 * @return
	 */
	public String queryMaxDate(){
		ARE.getLog().info("开始获取solr最新的一条数据");
		SolrQuery params=new SolrQuery();
		params.set("q", "serialno:MZCSZ*");
		params.set("fl", "collectiondate");
		params.set("sort", "collectiondate desc");
		params.setStart(0);
		params.setRows(1);
		QueryResponse response=null;
		try {
			response = server.query(params);
		} catch (SolrServerException e) {
			ARE.getLog().error("SOlr查询出错！", e);
		}
		SolrDocumentList list=response.getResults();
		if(list==null||list.size()==0||list.get(0)==null||list.get(0).get("collectiondate")==null){
			return "";
		}
		ARE.getLog().info("获取solr最新的一条数据:"+list.get(0).get("collectiondate").toString());
		return list.get(0).get("collectiondate").toString();
	}
	/**
	 * 根据ID删除索引
	 * @param serialNOs
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public void deleteByID(List<String> serialNOs) throws Exception{
		server.deleteById(serialNOs);
		server.commit();
	}
	
	/**
	 * 查询
	 * @param q
	 * @param pageNow
	 * @return
	 * @throws SolrServerException
	 */
	public QueryResponse queryDocuments(String q, int pageNow,int countPerPage)
			throws SolrServerException {
		SolrQuery query = new SolrQuery();	
		query.setRequestHandler("select");
		//query.setRequestHandler("/select");
		query.set("q",q);  //要检索字段由df制定，默认为copyfield text{hreftext,text}
		String lastIndexTime = ARE.getProperty("lastIndexTime");   //获取上次检索时间
		//query.set("fq","inputtime:["+lastIndexTime+"TO *]");   //增量检索
		query.setRows(countPerPage);// 每次取多少条
		query.setStart(pageNow);// 从第几条开始查询
		QueryResponse response = server.query(query);
		return response;
	}
	
	//删除所用索引
	public void deleteAllIndex() throws SolrServerException, IOException
	     {
//		  server.deleteByQuery("serialno:SZHB*");
//		
		System.out.println("删除!collectiondate:[2016/07/19 TO *] AND -datasource:*");
		server.deleteByQuery("collectiondate:[2016/07/19 TO *] AND -datasource:*");
          server.commit();        
	}
	/**
	 * 建立全文索引
	 */
	public void fullIndex()
	{
		SolrQuery query = new SolrQuery();
        // 指定RequestHandler，默认使用/select
        query.setRequestHandler("/dataimport");        
        query.setParam("command", "full-import")
             .setParam("clean", "true")
             .setParam("commit", "true")
             .setParam("optimize", "true");
        	 //.setParam("entity", "spider_data")
        try 
        {
            this.server.query(query);
        } catch (SolrServerException e) 
        {
        	ARE.getLog().error("建立全文索引失败！", e);
        }
	}
	/**
	 * 建立增量索引
	 */
    public void deltaIndex() {
        SolrQuery query = new SolrQuery();
        // 指定RequestHandler，默认使用/select
        query.setRequestHandler("/dataimport");        
        query.setParam("command", "delta-import")
             .setParam("clean", "false")
             .setParam("commit", "true")
             .setParam("optimize", "true");
        	 //.setParam("entity", "spider_data")
        try 
        {
            this.server.query(query);
        } catch (SolrServerException e) 
        {
        	ARE.getLog().error("建立索引失败！", e);
        }
    }

	public SolrServer getServer() {
		return server;
	}
}

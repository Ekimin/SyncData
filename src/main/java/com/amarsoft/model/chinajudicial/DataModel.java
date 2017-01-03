package com.amarsoft.model.chinajudicial;

/**
 * Created by ymhe on 2016/12/27.
 * 中国裁判文书网一级监控名单数据模型
 */
public class DataModel {
    private String serialNo;
    private String pDesc; //正文
    private String pType; //公告类型
    private String court; //法庭
    private String party;
    private String pDate;
    private String dataSource;
    private String caseNo;
    private String department;
    private String caseDate;
    private String plaintiff;
    private String agent;
    private String secretary;
    private String chiefJudge;
    private String judge;
    private String noticeAddress;
    private String docuClass;
    private String target;
    private String targetType;
    private String targetAmount;
    private String telNo;
    private String province;
    private String city;
    private String filePath;
    private String caseReason;
    private String collectionDate;
    private String dealDate;
    private String dealPerson;
    private String courtRoom;
    private String HTMLFilePath;
    private String qDate;
    private String documentClass;
    private String caseLevel;
    private String cardNo;
    private String IPName;
    //下面字段存在于27库
    private String isSynched; //是否需要同步。 N：尚未需要（默认），N：不需要（URL为空时，或者已经同步过的）
    //下面字段存在于25库
    private String status;// 标记是否同步：waiting：等待同步；invalid：被布隆过滤器过滤掉的；success：同步成功的
    // 下面字段不存在于数据库，是同步程序中用到的逻辑字段
    private String URLStatus;// 数据源标志位：URL已经存在于布隆过滤器中了则标记R（重复）, URL为空则标记E（Empty）, 不重复标记T。
    private String instNo; //机构编号
    private String instName; //机构名称

    public String getCourt() {
        return court;
    }

    public void setCourt(String court) {
        this.court = court;
    }

    public String getParty() {
        return party;
    }

    public void setParty(String party) {
        this.party = party;
    }

    public String getpDate() {
        return pDate;
    }

    public void setpDate(String pDate) {
        this.pDate = pDate;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getCaseNo() {
        return caseNo;
    }

    public void setCaseNo(String caseNo) {
        this.caseNo = caseNo;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getCaseDate() {
        return caseDate;
    }

    public void setCaseDate(String caseDate) {
        this.caseDate = caseDate;
    }

    public String getPlaintiff() {
        return plaintiff;
    }

    public void setPlaintiff(String plaintiff) {
        this.plaintiff = plaintiff;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public String getSecretary() {
        return secretary;
    }

    public void setSecretary(String secretary) {
        this.secretary = secretary;
    }

    public String getChiefJudge() {
        return chiefJudge;
    }

    public void setChiefJudge(String chiefJudge) {
        this.chiefJudge = chiefJudge;
    }

    public String getJudge() {
        return judge;
    }

    public void setJudge(String judge) {
        this.judge = judge;
    }

    public String getNoticeAddress() {
        return noticeAddress;
    }

    public void setNoticeAddress(String noticeAddress) {
        this.noticeAddress = noticeAddress;
    }

    public String getDocuClass() {
        return docuClass;
    }

    public void setDocuClass(String docuClass) {
        this.docuClass = docuClass;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetAmount() {
        return targetAmount;
    }

    public void setTargetAmount(String targetAmount) {
        this.targetAmount = targetAmount;
    }

    public String getTelNo() {
        return telNo;
    }

    public void setTelNo(String telNo) {
        this.telNo = telNo;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getCaseReason() {
        return caseReason;
    }

    public void setCaseReason(String caseReason) {
        this.caseReason = caseReason;
    }

    public String getCollectionDate() {
        return collectionDate;
    }

    public void setCollectionDate(String collectionDate) {
        this.collectionDate = collectionDate;
    }

    public String getDealDate() {
        return dealDate;
    }

    public void setDealDate(String dealDate) {
        this.dealDate = dealDate;
    }

    public String getDealPerson() {
        return dealPerson;
    }

    public void setDealPerson(String dealPerson) {
        this.dealPerson = dealPerson;
    }

    public String getCourtRoom() {
        return courtRoom;
    }

    public void setCourtRoom(String courtRoom) {
        this.courtRoom = courtRoom;
    }

    public String getHTMLFilePath() {
        return HTMLFilePath;
    }

    public void setHTMLFilePath(String HTMLFilePath) {
        this.HTMLFilePath = HTMLFilePath;
    }

    public String getqDate() {
        return qDate;
    }

    public void setqDate(String qDate) {
        this.qDate = qDate;
    }

    public String getDocumentClass() {
        return documentClass;
    }

    public void setDocumentClass(String documentClass) {
        this.documentClass = documentClass;
    }

    public String getCaseLevel() {
        return caseLevel;
    }

    public void setCaseLevel(String caseLevel) {
        this.caseLevel = caseLevel;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public String getIPName() {
        return IPName;
    }

    public void setIPName(String IPName) {
        this.IPName = IPName;
    }

    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }

    public String getCrawlerID() {
        return crawlerID;
    }

    public void setCrawlerID(String crawlerID) {
        this.crawlerID = crawlerID;
    }

    private String batchNo;
    private String crawlerID;

    public String getSerialNo() {
        return serialNo;
    }

    public void setSerialNo(String serialNo) {
        this.serialNo = serialNo;
    }

    public String getpDesc() {
        return pDesc;
    }

    public void setpDesc(String pDesc) {
        this.pDesc = pDesc;
    }

    public String getpType() {
        return pType;
    }

    public void setpType(String pType) {
        this.pType = pType;
    }

    public String getIsSynched() {
        return isSynched;
    }

    public void setIsSynched(String isSynched) {
        this.isSynched = isSynched;
    }

    public String getURLStatus() {
        return URLStatus;
    }

    public void setURLStatus(String URLStatus) {
        this.URLStatus = URLStatus;
    }

    public String getInstNo() {
        return instNo;
    }

    public void setInstNo(String instNo) {
        this.instNo = instNo;
    }

    public String getInstName() {
        return instName;
    }

    public void setInstName(String instName) {
        this.instName = instName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}

package com.amarsoft.model.chinaexecuted;

/**
 * Created by ryang on 2017/1/3.
 */

//被执行人数据库对象
public class ChinaExecutedModel {
    String SERIALNO;
    String ID;
    String PNAME;
    String CASECODE;
    String CASESTATE;
    String EXECCOURTNAME;
    String EXECMONEY;
    String PARTYCARDNUM;
    String CASECREATETIME;
    String SPIDERTIME;
    String ISINUSE;
    String INPUTTIME;
    String REALTIME;

    public String getSERIALNO(){
        return SERIALNO;
    }
    public void setSERIALNO(String serialno){
        this.SERIALNO = serialno;
    }





}

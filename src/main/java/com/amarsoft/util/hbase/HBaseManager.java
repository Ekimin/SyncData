package com.amarsoft.util.hbase;


import com.amarsoft.are.ARE;
import com.amarsoft.are.lang.StringX;
import com.amarsoft.model.common.FileModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Ekimin on 2016/12/28.
 * ChinaJudicialDataTransfer
 */
public class HBaseManager {
    private static String ZK_HOST;
    private static String ZK_PORT;
    private static String TABLE_NAME; //表名
    private static String FAMILY; //列簇
    private static String QUALIFIER;
    private static boolean isInit = false; //标志位：是否初始化

    public static Configuration conf = null;
    public static Connection conn = null;//注意链接使用后应及时关闭
    public static Table table = null;

    /**
     * 构造方法
     * <li>从are.xml中读取hbase配置信息，实例化对象</li>
     */
    public HBaseManager() {
        ARE.getLog().info("开始初始化HBase..............");
        init();
        ARE.getLog().info("初始化HBase完成..............");
    }

    private void init() {
        final String ZK_HOST = ARE.getProperty("ZK_HOST");
        final String ZK_PORT = ARE.getProperty("ZK_PORT");
        final String HTABLE = ARE.getProperty("HBASE_TABLE");
        final String HFAMILY = ARE.getProperty("HBASE_FAMILY");
        final String QUALIFIER = ARE.getProperty("QUALIFIER");

        if (ZK_HOST == null || ZK_PORT == null || HTABLE == null || HFAMILY == null || QUALIFIER == null) {
            ARE.getLog().error("请检查Hbase配置properties, 初始化hbase失败");
        } else {
            init(ZK_HOST, ZK_PORT, HTABLE, HFAMILY, QUALIFIER);
        }
    }

    /**
     * 指定配置信息，初始化hbase
     *
     * @param zkHost
     * @param zkPort
     * @param tableName
     * @param family
     * @param qualifier
     */
    public void init(String zkHost, String zkPort, String tableName, String family, String qualifier) {
        ZK_HOST = zkHost;
        ZK_PORT = zkPort;
        TABLE_NAME = tableName;
        FAMILY = family;
        QUALIFIER = qualifier;

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_HOST);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        isInit = true;

        ARE.getLog().info("zookeeper host:" + ZK_HOST);
        ARE.getLog().info("zookeeper port:" + ZK_PORT);
        ARE.getLog().info("表名:" + TABLE_NAME);
        ARE.getLog().info("列簇:" + FAMILY);
        ARE.getLog().info("列名:" + QUALIFIER);

    }

    /**
     * 建立一个hbase链接，并初始化Table
     *
     * @param tableName
     */
    public void getConnect(String tableName) {
        try {
            //ARE.getLog().info("开始建立connection  =====> tableName:" + tableName);
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭链接和Table
     */
    public void connClose() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历输出表中的值
     */
    public void scanTable() {
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result rs :
                    resultScanner) {
                try {
                    printResult(rs);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印Result对象中的值
     * @param result
     * @throws Exception
     */
    public static void printResult(Result result) throws Exception {
        for (Cell cell : result.rawCells()) {
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
            System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
            System.out.println(" 时间戳: " + cell.getTimestamp());
        }
    }

    /**
     * 批量存储数据
     *
     * @param fileModelList
     */
    public void saveBatch(List<FileModel> fileModelList) {
        Connection conn = null;
        Table table = null;
        try {
            //获取链接
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(TABLE_NAME));

            List<Put> putList = new LinkedList<Put>();
            Put put = null;
            String rowkey = null;
            String content = null;
            //批量存储
            for (FileModel fileModel : fileModelList) {
                rowkey = fileModel.getSerialno();
                content = fileModel.getContent();
                if (StringX.isEmpty(rowkey) || StringX.isEmpty(content)) {
                    continue; //跳过空值数据
                }
                put = new Put(rowkey.getBytes()); //通过rowkey添加数据
                put.addColumn(FAMILY.getBytes(), "PDESC".getBytes(), content.getBytes());
                putList.add(put);
            }
            table.put(putList);//批量提交
        } catch (IOException e) {
            ARE.getLog().error("批量存储Hbase出错了", e);
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException e) {
                ARE.getLog().error("存储Hbase数据后释放资源出错了！", e);
                e.printStackTrace();
            }

        }
    }


    /**
     * 获取hbase中数据
     * <li>该方法需要先建立Connection，和Table。使用于短时间多次读取数据</li>
     *
     * @param serialNo
     * @param field
     * @return
     */
    public String getValue(String serialNo, String field) {
        if (conn == null || table == null) {
            ARE.getLog().debug("请先建立connection和Table");
            return null;
        }

        String value = null;
        byte[] var;
        try {
            Get get = new Get(Bytes.toBytes(serialNo));
            Result result = table.get(get);
            var = result.getValue(FAMILY.getBytes(), field.getBytes());
            if (var != null) {
                value = new String(var);
            }
        } catch (IOException e) {
            ARE.getLog().info("从Hbase获取数据出错", e);
            e.printStackTrace();
        }


        return value;
    }


    /**
     * 通过流水号和field获取数据
     * <li>该方法不需要提前建立Hbase链接，适用于单次获取数据和用作测试</li>
     *
     * @param serialNo
     * @param field
     * @return
     */
    public String getValueByNo(String serialNo, String field) {
        Connection connection = null;
        Table table = null;
        byte[] var;
        String value = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(Bytes.toBytes(serialNo));
            Result result = table.get(get);
            var = result.getValue(FAMILY.getBytes(), field.getBytes());
            if (var != null) {
                value = new String(var);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关闭资源
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public static void main(String[] args) {
//        HBaseControl.testSolr();
        HBaseManager.testHbase("ZCSZ2016101400984166");
    }

    public static void testHbase(String serialno) {
        ARE.init();
        String zkHost = ARE.getProperty("ZK_HOST");
        String zkPort = ARE.getProperty("ZK_PORT");
        String HTABLE = ARE.getProperty("HBASE_TABLE");
        String HFAMILY = ARE.getProperty("HBASE_FAMILY");
        String QUALIFIER = ARE.getProperty("QUALIFIER");

        HBaseManager hBaseManager = new HBaseManager();
        String value = hBaseManager.getValueByNo(serialno, "content");

        System.out.println(value);

        //test put
//        List<FileModel> testList = new LinkedList<FileModel>();
//        FileModel dataModel = new FileModel();
//        dataModel.setSerialno("ZCSZCLOUD2016101400953346");
//        dataModel.setContent("test1" + new String(value));
//        testList.add(dataModel);
//
//        hBaseControl.saveBatch(testList);
    }

//    public static void testSolr() {
//        ARE.init();
//        String zkHost = ARE.getProperty("ZK_HOST");
//        String zkPort = ARE.getProperty("ZK_PORT");
//        String tableName = "courtbulletinCZ";
//        String family = "name";
//        String qualifier = "PDESC";
//
//        SolrManager solr = new SolrManager(ARE.getProperty("SOLRHOST"));
//        DataManager dm = new DataManager();
//        List<DataModel> list = new LinkedList<DataModel>();
//        DataModel dataModel = new DataModel();
//
//
//        dataModel.setSerialNo("ZCSZ2016102302591779");
//        dataModel.setpDesc("for test only, delete this when found");
//        list.add(dataModel);
//        HBaseControl hBaseControl = new HBaseControl();
//        dm.syncSolr(list, solr, hBaseControl);
//    }

    public static String getZkHost() {
        return ZK_HOST;
    }

    public static void setZkHost(String zkHost) {
        ZK_HOST = zkHost;
    }

    public static String getZkPort() {
        return ZK_PORT;
    }

    public static void setZkPort(String zkPort) {
        ZK_PORT = zkPort;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    public static void setTableName(String tableName) {
        TABLE_NAME = tableName;
    }

    public static String getFAMILY() {
        return FAMILY;
    }

    public static void setFAMILY(String FAMILY) {
        HBaseManager.FAMILY = FAMILY;
    }

    public static String getQUALIFIER() {
        return QUALIFIER;
    }

    public static void setQUALIFIER(String QUALIFIER) {
        HBaseManager.QUALIFIER = QUALIFIER;
    }

    public static boolean isIsInit() {
        return isInit;
    }

    public static void setIsInit(boolean isInit) {
        HBaseManager.isInit = isInit;
    }
}

package com.amarsoft.util.bloomfilter.imp;

import com.amarsoft.are.ARE;
import com.amarsoft.util.bloomfilter.BloomFilter;
import com.amarsoft.util.bloomfilter.BloomFilterManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * 布隆过滤管理器默认实现
 *
 * @author dwyang
 */
public class DefaultBloomFilterManager implements BloomFilterManager {

    private static BloomFilter<String> bloomFilter;//布隆过滤器
    private static String path;//存放布隆过滤器状态的文件路径
    private static boolean isInit;

    public boolean isContain(String url) {
        return bloomFilter.contains(url);
    }

    public boolean add(String url) {
        return bloomFilter.add(url);
    }

    public synchronized void init() {
        if (isInit) {
            return;
        }
        //初始化布隆过滤器
        path = ARE.getProperty("PATH");
        ARE.getLog().info("~~~~~~~~~~~~~APP_HOME = " + ARE.getProperty("APP_HOME"));
        ARE.getLog().info("~~~~~~~~~~~~~PATH = " + path);
        File file = new File(path);
        File zipFile = new File(path + ".zip");
        if (!file.exists()) {
            extractZip(zipFile, path);
        }
        bloomFilter = new BloomFilter<String>(file);
        isInit = true;
    }

    /**
     * 将zip文件解压到制定的文件路径
     *
     * @param fileZip zip文件
     * @param path 解压后的文件路径
     */
    private void extractZip(File fileZip, String path) {
        ZipInputStream zin = null;
        FileOutputStream out = null;
        try {
            ARE.getLog().info("read bloom.zip[" + fileZip.getAbsolutePath() + "]");
            zin = new ZipInputStream(new FileInputStream(fileZip));//输入源zip路径
            ZipEntry entry = zin.getNextEntry();
            if (entry != null) {
                out = new FileOutputStream(path);
                byte[] buf = new byte[102400];
                int b = -1;
                while ((b = zin.read(buf)) != -1) {
                    out.write(buf, 0, b);
                }
                out.flush();
                ARE.getLog().info("extra bloom.zip[" + fileZip.getAbsolutePath() + "] success");
            } else {
                ARE.getLog().info("bloom.zip[" + fileZip.getAbsolutePath() + "] is empty");
            }
        } catch (Exception e) {
            ARE.getLog().error("extra bloom.zip[" + fileZip.getAbsolutePath() + "] fail", e);
        } finally {
            try {
                if (zin != null) zin.close();
                if (out != null) out.close();
            } catch (Exception e) {
                ARE.getLog().error(e.toString(), e);
            }

        }
    }

    public void save() {
        bloomFilter.save(path);
    }

    /**
     * 判断布隆过滤器Manager是否已经初始化
     * @return
     */
    public boolean isInit() {
        return isInit;
    }

}

package com.amarsoft.util.common;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ymhe on 2017/1/3.
 * 日期类
 */
public class DateManager {

    /**
     * 获取当前系统时间，默认格式yyyy/MM/dd HH:mm:ss
     * @return
     */
    public static String getCurrentDate(){
        String formatString = "yyyy/MM/dd HH:mm:ss"; //默认
        return formatDate(formatString);
    }

    /**
     * 获取当前系统时间，格式有formatString指定
     * @param formatString 时间格式，如yyyy/MM/dd HH:mm:ss
     * @return
     */
    public static String getCurrentDate(String formatString){
        return formatDate(formatString);
    }

    private static String formatDate(String formatString) {
        String dateString = "";
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatString);
        dateString = simpleDateFormat.format(date);

        return dateString;
    }

    public static void main(String arg[]) {
        System.out.print(getCurrentDate());
    }
}

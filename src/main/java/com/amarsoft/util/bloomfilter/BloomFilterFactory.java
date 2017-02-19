package com.amarsoft.util.bloomfilter;


import com.amarsoft.util.bloomfilter.imp.DefaultBloomFilterManager;

import java.util.HashMap;
import java.util.Map;


/***
 * 布隆过滤器工厂类
 * @author dwyang
 * */
public class BloomFilterFactory {
	private static Map<String,BloomFilterManager> managers = new HashMap<String,BloomFilterManager>();

	private BloomFilterFactory(){}

	/**
	 * 获取默认的布隆过滤管理器
	 * */
	public static BloomFilterManager getDefaultBloomFiterManager(){
		return getBloomFiterManager("com.amarsoft.court.app.bloomfilter.imp.DefaultBloomFilterManager",new DefaultBloomFilterManager());
	}


	/**
	 * 获取指定的布隆过滤管理器
	 * */
	public static BloomFilterManager getBloomFilterManager(String skey) {
		return getBloomFiterManager(skey,new DefaultBloomFilterManager());
	}

	/**
	 * 获取指定的管理器，如果不存在，则返回给定的默认管理器
	 * @param skey 管理器对应的key
	 * @author 指定的默认管理器
	 * @return
	 * */
	private static BloomFilterManager getBloomFiterManager(String skey,BloomFilterManager defaulManager){
		BloomFilterManager manager = managers.get(skey);
		if (manager == null) {
			managers.put(skey, defaulManager);
		}
		manager = defaulManager;
		return manager;
	}

}

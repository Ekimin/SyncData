package com.amarsoft.util.bloomfilter;

/**
 * 布隆过滤管理器
 * @author dwyang
 * */
public interface BloomFilterManager {

	/**
	 * 检查给定的url是否存在
	 * */
	public boolean isContain(String url);

	/**
	 * 添加url到布隆过滤器中
	 * */
	public boolean add(String url);

	/**
	 * 初始化布隆过滤器
	 * */
	public void init();

	/**
	 * 用于检查布隆过滤器是否已经初始化
	 * @return true if init,else false
	 * */
	public boolean isInit();

	/**
	 * 保存布隆过滤器的状态
	 * */
	public void save();
}

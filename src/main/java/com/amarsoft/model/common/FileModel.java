package com.amarsoft.model.common;

/**
 * 文件模型
 * */
public class FileModel {
	
	private String serialno;
	
	private String path;
	
	private String content;
	
	public FileModel(){}
	
	public FileModel(String serialno, String path, String content) {
		this.serialno = serialno;
		this.path = path;
		this.content = content;
	}

	public String getSerialno() {
		return serialno;
	}

	public void setSerialno(String serialno) {
		this.serialno = serialno;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	

}

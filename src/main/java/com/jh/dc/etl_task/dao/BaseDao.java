package com.jh.dc.etl_task.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;

@Component
public class BaseDao {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	@Qualifier("targetDDJdbcTemplate")
	JdbcTemplate targetDDJdbcTemplate;
	
	@Autowired
	@Qualifier("targetDTJdbcTemplate")
	JdbcTemplate targetDTJdbcTemplate;
	
	@Autowired
	@Qualifier("sourceDDJdbcTemplate")
	JdbcTemplate sourceDDJdbcTemplate;
	
	@Autowired
	@Qualifier("sourceDTJdbcTemplate")
	JdbcTemplate sourceDTJdbcTemplate;

	public JdbcTemplate getJdbcTemplate(String db){
		if(db.equals("targetDT")){
			return targetDTJdbcTemplate;
		}else if(db.equals("targetDD"))
			return targetDDJdbcTemplate;
		else if(db.equals("sourceDT"))
			return sourceDTJdbcTemplate;
		else if(db.equals("sourceDD"))
			return sourceDDJdbcTemplate;
		else {
			throw new RuntimeException("数据库标识不存在:"+db);
		}
	}
	
	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param columns 列
	 * @param table 表明
	 * @param targetDS 目标数据库
	 */
	public void insertOrUpdate(List<Map<String,Object>> datas,String[] columns,String table,String targetDS){
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		sql.append(" ON DUPLICATE KEY UPDATE ");
		for(int i=0;i<columns.length;i++){
			if(i>0)
				sql.append(",");
			sql.append(columns[i]+"=values("+columns[i]+")");
		}
		String[] ds = targetDS.split(",");
		int pageSize = datas.size()/1000;
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			int i=0;
			for(;i<pageSize;i++){
				logger.info("保存"+table+"数据,"+pageSize+"----"+i);
				batchUpdate(sql.toString(),datas.subList(i*1000, (i+1)*1000),jdbc,columns);
			}
			batchUpdate(sql.toString(),datas.subList((i)*1000,datas.size()),jdbc,columns);
		}
	}
	
	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param columns 列
	 * @param table 表明
	 */
	public void insertOrUpdate(List<Map<String,Object>> datas,String[] columns,String table,JdbcTemplate jdbc){
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		sql.append(" ON DUPLICATE KEY UPDATE ");
		for(int i=0;i<columns.length;i++){
			if(i>0)
				sql.append(",");
			sql.append(columns[i]+"=values("+columns[i]+")");
		}
		int pageSize = datas.size()/1000;
		int i=0;
		for(;i<pageSize;i++){
			logger.info("保存"+table+"数据,"+pageSize+"----"+i);
			batchUpdate(sql.toString(),datas.subList(i*1000, (i+1)*1000),jdbc,columns);
		}
		batchUpdate(sql.toString(),datas.subList((i)*1000,datas.size()),jdbc,columns);
	}
	
	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param columns 列
	 * @param table 表明
	 * @param targetDS 目标数据库
	 */
	public void insert(List<Map<String,Object>> datas,String[] columns,String table,String targetDS){
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		String[] ds = targetDS.split(",");
		int pageSize = datas.size()/1000;
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			int i=0;
			for(;i<pageSize;i++){
				logger.info("保存"+table+"数据,"+pageSize+"----"+i);
				batchUpdate(sql.toString(),datas.subList(i*1000, (i+1)*1000),jdbc,columns);
			}
			batchUpdate(sql.toString(),datas.subList((i)*1000,datas.size()),jdbc,columns);
		}
	}
	
	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param columns 列
	 * @param table 表明
	 */
	public void insert(List<Map<String,Object>> datas,String[] columns,String table,JdbcTemplate jdbc){
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		int pageSize = datas.size()/1000;
		int i=0;
		for(;i<pageSize;i++){
			logger.info("保存"+table+"数据,"+pageSize+"----"+i);
			batchUpdate(sql.toString(),datas.subList(i*1000, (i+1)*1000),jdbc,columns);
		}
		batchUpdate(sql.toString(),datas.subList((i)*1000,datas.size()),jdbc,columns);
	}
	
	void batchUpdate(String sql,List<Map<String,Object>> datas ,JdbcTemplate jdbc,String[] columns){
		jdbc.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement ps, int index) throws SQLException {
				Map<String,Object> data = datas.get(index);
				for(int i=0;i<columns.length;i++){
					ps.setObject(i+1, data.get(columns[i]));
				}
			}
			
			@Override
			public int getBatchSize() {
				// TODO Auto-generated method stub
				return datas.size();
			}
		});
	}
	
	void batchUpdate(String sql,List<Map<String,Object>> datas ,JdbcTemplate jdbc,Map<String,String> columnAlias){
		Object[] srcColumns = columnAlias.keySet().toArray();
		jdbc.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement ps, int index) throws SQLException {
				Map<String,Object> data = datas.get(index);
				for(int i=0;i<srcColumns.length;i++){
					ps.setObject(i+1, data.get(srcColumns[i].toString()));
				}
			}
			
			@Override
			public int getBatchSize() {
				// TODO Auto-generated method stub
				return datas.size();
			}
		});
	}

	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param table 表明
	 * @param targetDS 目标数据库
	 */
	public void insertOrUpdate(List<Map<String,Object>> datas,Map<String,String> columnAlias,String table,String targetDS){
		Object[] columns =  columnAlias.values().toArray();
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(") ON DUPLICATE KEY UPDATE ");
		for(int i=0;i<columns.length;i++){
			if(i>0)
				sql.append(",");
			sql.append(columns[i]+"=values("+columns[i]+")");
		}
		
		String[] ds = targetDS.split(",");
		int pageSize = datas.size()/1000;
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			int i=0;
			for(;i<pageSize;i++){
				logger.info("保存"+table+"数据,"+d+pageSize+"----"+i);
				batchUpdate(sql.toString(),datas.subList(i*1000, (i+1)*1000),jdbc,columnAlias);
			}
			batchUpdate(sql.toString(),datas.subList((i)*1000,datas.size()),jdbc,columnAlias);
		}
		
		/*
		String[] ds = targetDS.split(",");
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			jdbc.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
				
				@Override
				public void setValues(PreparedStatement ps, int index) throws SQLException {
					Map<String,Object> data = datas.get(index);
					for(int i=0;i<srcColumns.length;i++){
						ps.setObject(i+1, data.get(srcColumns[i].toString()));
					}
				}
				
				@Override
				public int getBatchSize() {
					// TODO Auto-generated method stub
					return datas.size();
				}
			});
		}*/
	}
	
	
	/**
	 * 插入或更新数据
	 * @param datas 数据
	 * @param table 表明
	 * @param targetDS 目标数据库
	 */
	public void insert(List<Map<String,Object>> datas,Map<String,String> columnAlias,String table,String targetDS){
		Object[] columns =  columnAlias.values().toArray();
		Object[] srcColumns = columnAlias.keySet().toArray();
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		String[] ds = targetDS.split(",");
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			jdbc.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
				
				@Override
				public void setValues(PreparedStatement ps, int index) throws SQLException {
					Map<String,Object> data = datas.get(index);
					for(int i=0;i<srcColumns.length;i++){
						ps.setObject(i+1, data.get(srcColumns[i].toString()));
					}
				}
				
				@Override
				public int getBatchSize() {
					// TODO Auto-generated method stub
					return datas.size();
				}
			});
		}
	}
	
	/**
	 * 插入或更新数据 
	 * @param datas 数据数组
	 * @param columns 列
	 * @param table 表名
	 * @param targetDS 目标数据库
	 */
	public void insertOrUpdateList(List<ArrayList<Object>> datas,String[] columns,String table,String targetDS){
		StringBuffer sql = new StringBuffer("insert into "+table+"(");
		sql.append(StringUtils.join(columns, ","));
		sql.append(") values (");
		for(int i=0;i<columns.length;i++){
			if(i>0)sql.append(",");
			sql.append("?");
		}
		sql.append(") ON DUPLICATE KEY UPDATE ");
		for(int i=0;i<columns.length;i++){
			if(i>0)
				sql.append(",");
			sql.append(columns[i]+"=values("+columns[i]+")");
		}
		String[] ds = targetDS.split(",");
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			jdbc.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
				
				@Override
				public void setValues(PreparedStatement ps, int index) throws SQLException {
					ArrayList data = datas.get(index);
					for(int i=0;i<columns.length;i++){
						ps.setObject(i+1, data.get(i));
					}
				}
				
				@Override
				public int getBatchSize() {
					// TODO Auto-generated method stub
					return datas.size();
				}
			});
		}
	}


	public void updateSql_targetDS(String delSql, String targetDS, Object... param) {
		String[] ds = targetDS.split(",");
		for(String d : ds){
			JdbcTemplate jdbc = getJdbcTemplate(d);
			jdbc.update(delSql,param);
		}
	}
	
	private String concatCondition(String[] keys){
		StringBuffer sb = new StringBuffer("");
		for(int i=0;i<keys.length;i++){
			if(i>0)
				sb.append(" and ");
			sb.append(keys[i]+"=?");
		}
		return sb.toString();
	}
	
	class UpdateSetter implements PreparedStatementSetter{
		
		final Map<String,Object> params ;
		final String[] keys;
		final String[] columns;
		
		public UpdateSetter(Map<String,Object> param,String[] targetKey,String[] columns){
			params=param;
			keys=targetKey;
			this.columns=columns;
		}
		
		@Override
		public void setValues(PreparedStatement arg0) throws SQLException {
			int i=0;
			for(;i<columns.length;i++){
				arg0.setObject(i+1, params.get(columns[i]));
			}
			for(int j=0;j<keys.length;j++,i++){
				arg0.setObject(i+1, params.get(keys[j]));
			}
		}
	}

}

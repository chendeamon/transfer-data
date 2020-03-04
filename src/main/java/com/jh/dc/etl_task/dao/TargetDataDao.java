package com.jh.dc.etl_task.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.catalina.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;

@Component
public class TargetDataDao extends BaseDao{

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	/**
	 * 保存目标数据
	 * @param toDatas
	 * @param table
	 */
	public void saveDatas(List<LinkedHashMap<String, Object>> toDatas,
			Map<String, Object> table,List<Map<String,Object>> columns) {
		//拆分数据源
		String[] toDataSources = table.get("target_data_source").toString().split(",");
		for(String db : toDataSources){
			logger.info("保存到："+db);
			JdbcTemplate jdbc = getJdbcTemplate(db);
			String targetKey=table.get("target_key").toString();
			String sqlCount = "select count(1) from "+table.get("target_table")+" where "+concatCondition(targetKey);
			Object updateModel=table.get("update_model");
			//boolean needInsert = table.get("update_model")==null || "0".equals(table.get("update_model").toString()); 
			StringBuffer sqlUpdate = new StringBuffer("update "+table.get("target_table") +" set ");
			StringBuffer sqlInsert = new StringBuffer("insert into "+table.get("target_table")+" (");
//			StringBuffer sqlInsert = new StringBuffer("replace into "+table.get("target_table")+" (");
			StringBuffer valStr=new StringBuffer(" values (");
			
			StringBuffer sqlOnKeyUpdate = new StringBuffer(" ON DUPLICATE KEY UPDATE ");//暂时没用上
			for(int i=0;i<columns.size();i++){
				if(i>0){
					sqlInsert.append(",");
					valStr.append(",");
					sqlUpdate.append(",");
					sqlOnKeyUpdate.append(",");
				}
				valStr.append("?");
				sqlInsert.append(columns.get(i).get("target_column"));
				sqlUpdate.append(columns.get(i).get("target_column")+"=?");
				sqlOnKeyUpdate.append(columns.get(i).get("target_column")+"=values("+columns.get(i).get("target_column")+")");
			}
			//插入语句，加上逻辑主键字段
			String[] keys = targetKey.split(",");
			/*for(int i=0;i<keys.length;i++){
				valStr.append(",?");
				sqlInsert.append(","+keys[i]);
			}*/
			valStr.append(")");
			sqlInsert.append(")");
			sqlUpdate.append(" where ").append(concatCondition(targetKey));
			sqlInsert.append(valStr);
			logger.info("保存数据：total:"+toDatas.size());
			//如果是先删除再插入，得在分页前执行！！！！！！！！！！！
			for(int i=0;i<toDatas.size();i++){
				ArrayList<Object> keyVal = new ArrayList<Object>();
				//存在函数  按照;分割  否则按照,分割
				if(table.get("target_key").toString().indexOf("(")==-1){
					keys = table.get("target_key").toString().split(",");
				}else{
					keys = table.get("target_key").toString().split(";");
				}
				for(int j=0;j<keys.length;j++){
					keyVal.add(toDatas.get(i).get(keys[j]));
				}
				if(updateModel==null ||updateModel.toString().length()==0 || "0".equals(updateModel.toString())){//插入更新时，先判断是否有数据
					
					if(jdbc.queryForObject(sqlCount, Integer.class,keyVal.toArray())>0){
						//有数据才更新
						jdbc.update(sqlUpdate.toString(),new UpdateSetter(toDatas.get(i),targetKey,columns));
					}else{
						jdbc.update(sqlInsert.toString(), new InsertSetter(toDatas.get(i),columns));
					}
					
				}else if("1".equals(updateModel.toString())){//仅更新模式，只执行更新
					jdbc.update(sqlUpdate.toString(),new UpdateSetter(toDatas.get(i),targetKey,columns));
				}else if("2".equals(updateModel.toString())){//直接插入
					jdbc.update(sqlInsert.toString(), new InsertSetter(toDatas.get(i),columns));
				}else if("3".equals(updateModel.toString())){//直接插入  之前也不删除
					jdbc.update(sqlInsert.toString(), new InsertSetter(toDatas.get(i),columns));
				}
				
//				jdbc.update(sqlInsert.toString(), new InsertSetter(toDatas.get(i)));
				if(i%1000==0){
					logger.info("保存数据：current:"+i);
				}
			}
		}
	}
	
	
	
	/**
	 * 拼接条件 sql
	 * @param key
	 * @return
	 */
	private String concatCondition(String key){
		//存在函数  按照;分割  否则按照,分割
		if(key.indexOf("(")==-1){
			//没有函数
			if(key.indexOf(",")==-1){
				return key+"= ? ";
			}else{
				String[] keys = key.split(",");
				StringBuffer sb = new StringBuffer("");
				for(int i=0;i<keys.length;i++){
					if(i>0)
						sb.append(" and ");
					sb.append(keys[i]+"=?");
				}
				return sb.toString();
			}
		}else{
			if(key.indexOf(";")==-1){
				return key+"= ? ";
			}else{
				String[] keys = key.split(";");
				StringBuffer sb = new StringBuffer("");
				for(int i=0;i<keys.length;i++){
					if(i>0)
						sb.append(" and ");
					if(keys[i].indexOf("as")>0){
						keys[i] = keys[i].substring(0, keys[i].indexOf("as"));
					}
					sb.append(keys[i]+"=?");
				}
				return sb.toString();
			}
		}
		
		
	}
	
	/**
	 * 插入
	 * @author Jason
	 *
	 */
	class InsertSetter implements PreparedStatementSetter{
		
		final Map<String,Object> params ;
		final List<Map<String,Object>> columns;
		
		public InsertSetter(Map<String,Object> param,List<Map<String,Object>> columns){
			params=param;
			this.columns=columns;
		}
		@Override
		public void setValues(PreparedStatement arg0) throws SQLException {
			int i=0;
			for(;i<columns.size();i++){
				arg0.setObject(i+1, params.get(columns.get(i).get("target_column")));
			}
		}
	}
	
	/**
	 * 取条数
	 * @author Jason
	 *
	 */
	class CountSetter implements PreparedStatementSetter{
		
		final Map<String,Object> params ;
		final String key;
		
		public CountSetter(Map<String,Object> param,String key){
			params=param;
			this.key=key;
		}
		@Override
		public void setValues(PreparedStatement arg0) throws SQLException {
			String[] keys = key.split(",");
			for(int i=0;i<keys.length;i++){
				arg0.setObject(i+1, params.get(i));
			}
		}
		
	}
	
	class UpdateSetter implements PreparedStatementSetter{
		
		final Map<String,Object> params ;
		final String key;
		final List<Map<String,Object>> columns;
		
		public UpdateSetter(Map<String,Object> param,String targetKey,List<Map<String,Object>> columns){
			params=param;
			key=targetKey;
			this.columns=columns;
		}
		@Override
		public void setValues(PreparedStatement arg0) throws SQLException {
			int i=0;
			for(;i<columns.size();i++){
				arg0.setObject(i+1, params.get(columns.get(i).get("target_column")));
			}
			String[] keys = key.split(",");
			if(key.indexOf("(")>0){
				//有函数  根据;分割
				keys = key.split(";");
			}
			for(int j=0;j<keys.length;j++,i++){
				arg0.setObject(i+1, params.get(keys[j]));
			}
			/*Object[] vals = params.values().toArray();
			for(int i=0;i<vals.length;i++){
				arg0.setObject(i+1, vals[i]);
			}*/
		}
		
	}
	
}

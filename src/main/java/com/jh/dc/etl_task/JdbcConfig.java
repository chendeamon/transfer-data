package com.jh.dc.etl_task;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class JdbcConfig {

	@Bean
	@Primary
	@ConfigurationProperties(prefix = "spring.datasource.sourceDT")
	public DataSource sourceDT(){
	    return DataSourceBuilder.create().build();
	} 
	@Bean
	public JdbcTemplate sourceDTJdbcTemplate(@Qualifier("sourceDT") DataSource sourceDT) {
	    return new JdbcTemplate(sourceDT);
	} 
	
	@Bean
	@ConfigurationProperties(prefix = "spring.datasource.sourceDD")
	public DataSource sourceDD(){
	    return DataSourceBuilder.create().build();
	}  	
	@Bean
	public JdbcTemplate sourceDDvJdbcTemplate(@Qualifier("sourceDD") DataSource sourceDD) {
	    return new JdbcTemplate(sourceDD);
	}
	
	
	@Bean
	@ConfigurationProperties(prefix = "spring.datasource.targetDT")
	public DataSource targetDT(){
	    return DataSourceBuilder.create().build();
	} 
	@Bean
	public JdbcTemplate targetDTJdbcTemplate(@Qualifier("targetDT") DataSource targetDT) {
	    return new JdbcTemplate(targetDT);
	} 
	
	
	@Bean
	@ConfigurationProperties(prefix = "spring.datasource.targetDD")
	public DataSource targetDD(){
	    return DataSourceBuilder.create().build();
	}  	
	@Bean
	public JdbcTemplate targetDDJdbcTemplate(@Qualifier("targetDD") DataSource targetDD) {
	    return new JdbcTemplate(targetDD);
	}
 

}

package com.jh.dc.etl_task.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtils {

	public static String FROMT1="yyyy-MM-dd HH:mm:ss"; 
	public static String FROMT2="yyyy-MM-dd HH:mm"; 
	public static String FROMT3="yyyy-MM-dd"; 
	public static String FROMT4="HH:mm:ss"; 
	public static String FROMT5="yyyy年MM月dd�?"; 
	public static String FROMT6="yyyy.MM"; 
	public static String FROMT7="yyyyMMddHHmmss"; 
	public static String FROMT8="yyyyMMdd"; 
	public static String FROMT9="yyyyMM"; 
	public static String FROMT10="yyyy-M-d"; 
	public static String FROMT11="yyyy-MM"; 
	public static String FROMT12="yyyy/M/d";
	/**
	 * 字符串日期转Date类型
	 * @param date
	 * @param fromt
	 * @return
	 * @throws Exception
	 */
	public static Date strToDate(String date,String fromt){
		SimpleDateFormat format = new SimpleDateFormat(fromt);
		try {
			return format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * date转str
	 * @param date
	 * @param fromt
	 * @return
	 * @throws Exception
	 */
	public static String dateToStr(Date date,String fromt){
		SimpleDateFormat format = new SimpleDateFormat(fromt);
		return format.format(date);
	}
	
	/** 
	* 获得指定日期的前�?�? 
	* @param specifiedDay 
	* @return 
	* @throws Exception 
	*/ 
	public static String getSpecifiedDayBefore(String specifiedDay,String formt) throws Exception{ 
		//SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
		Calendar c = Calendar.getInstance(); 
		Date date=new SimpleDateFormat(formt).parse(specifiedDay); 
		c.setTime(date); 
		int day=c.get(Calendar.DATE); 
		c.set(Calendar.DATE,day-1); 
		String dayBefore=new SimpleDateFormat(formt).format(c.getTime()); 
		return dayBefore; 
	} 
	
	/** 
	* 获得指定日期的后�?�? 
	* @param specifiedDay 
	* @return 
	*/ 
	public static String getSpecifiedDayAfter(String specifiedDay,String formt) throws Exception{ 
		Calendar c = Calendar.getInstance(); 
		Date date=null; 
		date = new SimpleDateFormat(formt).parse(specifiedDay); 
		c.setTime(date); 
		int day=c.get(Calendar.DATE); 
		c.set(Calendar.DATE,day+1); 
		String dayAfter=new SimpleDateFormat(formt).format(c.getTime()); 
		return dayAfter; 
	}
	/**
	* 得到二个日期间的间隔天数
	* sj1-sj2
	*/
	public static Integer getTwoDay(String sj1, String sj2,String formt) throws Exception{
		SimpleDateFormat myFormatter = new SimpleDateFormat(formt);
		long day = 0;
		java.util.Date date = myFormatter.parse(sj1);
		java.util.Date mydate = myFormatter.parse(sj2);
		day = (date.getTime() - mydate.getTime()) / (24 * 60 * 60 * 1000);
		return Integer.parseInt(String.valueOf(day));
	}
	
	/**
	 * 获取过去的秒�?
	 * 
	 * @param date
	 * @return
	 */
	public static long pastSS(Date date) {
		long t = new Date().getTime() - date.getTime();
		return t /1000;
	}
	
	/**
	 * 时间加减day�?
	 * @param day
	 * @return
	 */
	public static String toDateString(int day){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, day);
		return new SimpleDateFormat(FROMT3).format(calendar.getTime());
	}
	
	/**
	 * 时间加减day�?
	 * @param day
	 * @return
	 */
	public static String addDate(String date,int day){
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(new SimpleDateFormat(FROMT3).parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		calendar.add(Calendar.DAY_OF_MONTH, day);
		return new SimpleDateFormat(FROMT3).format(calendar.getTime());
	}
	
	/**
	 * 时间加减month�?
	 * @param day
	 * @return
	 */
	public static String addDateMonth(String date,int month){
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(new SimpleDateFormat(FROMT3).parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		calendar.add(Calendar.MONTH, month);
		return new SimpleDateFormat(FROMT3).format(calendar.getTime());
	}
	
	/**
	 * 数据转换成�??2017HY','2016FY"的形�?
	 * @param num
	 * @return
	 */
	public static String dateToYear(Integer num){
		String str=dateToStr(new Date(),FROMT3);
		String sql="";
		while(num>0){
			String month=str.substring(5,7);
			if("07".equals(month)){
				String year=str.substring(0,4);
				sql+="'"+year+"HY"+"',";
				num--;
			}else if("01".equals(month)){
				String year=str.substring(0,4);
				int y=Integer.parseInt(year);
				y=y-1;
				sql+="'"+y+"FY"+"',";
				num--;
			}
			str=DateUtils.addDateMonth(str,-1);
		}
		sql=sql.substring(0,sql.length()-1);
		return sql;
	}
	
	/**
	 * 将日期转换成哪一年哪�?�?
	 * @param date 2017-04-15
	 * @return 2017-15
	 */
	@SuppressWarnings("static-access")
	public static String dateToWeek(String date){
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(new SimpleDateFormat(FROMT3).parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Integer week = cal.get(cal.WEEK_OF_YEAR);
		Integer year = cal.get(cal.YEAR);
		String weekStr = (week < 10 ? ("0" + week) : week.toString());
		return year + "-" + weekStr;
	}
	
	/**
	 * 通过日期获取月的天数
	 * @param date yyyy-MM
	 * @return
	 */
	public static Integer getDayByMouth(String date){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM"); 
		Calendar calendar = new GregorianCalendar();
		Date date1;
		try {
			date1 = sdf.parse(date);
			calendar.setTime(date1); //放入你的日期 
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
	}
	/**
	 * 查询某一天是星期�?
	 * @param day  日期
	 * @return
	 * @throws ParseException 
	 */
	public static String queryDayOfWeek(String day) throws ParseException{
		return new SimpleDateFormat("EEEE").format(strToDate(day, "yyyy-mm-dd"));
	}
	
	/**
	 * 获取当前时间
	 * @param format  格式
	 * @return
	 * @throws ParseException 
	 */
	public static String getToday(String format) {
		SimpleDateFormat df = new SimpleDateFormat(format);//设置日期格式
		return df.format(new Date());
	}
	/**
	 * 
	 * 判断日期大小
	 */
public static int compare_date(String DATE1, String DATE2) {
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() > dt2.getTime()) {
            	//date1 在date2 之后
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
            	//date2在date1 之后
                return -1;
            } else {
            	//相同
                return 0;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }
	/**
	 * 获取当前日期三个月之后的日期
	 */
	public static String countMonth(int month){
	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
	Calendar c = Calendar.getInstance();
	  System.out.println(f.format(c.getTime()));
	  c.add(Calendar.MONTH, month);
	  return f.format(c.getTime());
	}
	
	//获取上月月数
	public static  String getLastMonth(){
		Calendar c = Calendar.getInstance();
		c.add(Calendar.MONTH, -1);
		SimpleDateFormat format =  new SimpleDateFormat(FROMT11);
		String time = format.format(c.getTime());
		return time;
	}
}

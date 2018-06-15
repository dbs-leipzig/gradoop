package org.gradoop.flink.io.impl.rdbms.connect;

public class DriverClassNameChooser {
	public static String choose(String url){
		String driverClassName = null;
		if(url.contains("jdbc:mysql:")){
			driverClassName = "com.mysql.jdbc.Driver";
		}
		if(url.contains("jdbc:postgres:")){
			driverClassName = "org.postgresql.Driver";
		}
		if(url.contains("jdbc:h2:")){
			driverClassName = "org.h2.Driver";
		}
		if(url.contains("jdbc:sqlite:")){
			driverClassName = "org.sqlite.JDBC";
		}
		return driverClassName;
	}
}

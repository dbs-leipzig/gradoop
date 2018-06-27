package org.gradoop.flink.io.impl.rdbms.connection;

/**
 * simple parsing from given jdbc url to jdbc Drivername 
 * @author pc
 *
 */
public class DriverClassNameChooser {
	public static String choose(String url){
		String driverClassName = null;
		if(url.contains("jdbc:mysql:")){
			driverClassName = "com.mysql.jdbc.Driver";
		}
		if(url.contains("jdbc:postgresql:")){
			driverClassName = "org.postgresql.Driver";
		}
		if(url.contains("jdbc:h2:")){
			driverClassName = "org.h2.Driver";
		}
		if(url.contains("jdbc:sqlite:")){
			driverClassName = "org.sqlite.JDBC";
		}
		if(url.contains("jdbc:mariadb:")) {
			driverClassName = "org.mariadb.jdbc.Driver";
		}
		return driverClassName;
	}
}

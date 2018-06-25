package org.gradoop.flink.io.impl.rdbms.connection;

/**
 * simple parsing from given jdbc url to provided driver jar name 
 * @author pc
 *
 */
public class DriverFileNameChooser {
	public static String choose(String url){
		String driverName = null;
		if(url.contains("jdbc:mysql:")){
			driverName = "mysql-connector-java-5.1.46.jar!/";
		}
		if(url.contains("jdbc:postgresql:")){
			driverName = "postgresql-42.2.2.jar!/";
		}
		if(url.contains("jdbc:h2:")){
			driverName = "h2-1.4.197.jar!/";
		}
		if(url.contains("jdbc:sqlite:")){
			driverName = "sqlite-jdbc-3.23.1.jar!/";
		}
		return driverName;
	}
}

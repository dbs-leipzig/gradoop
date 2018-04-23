package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectToRDBMS {
	private Connection connection = null;
	
	public ConnectToRDBMS(String dbmsName, String url) throws SQLException{
		this.connection = estabslishSimpleConnection(driverSelection(dbmsName),url);
	}
	public ConnectToRDBMS(String dbmsName,String url, String user, String pw) throws SQLException{
		this.connection = establishConnection(driverSelection(dbmsName),url,user,pw);
	}
	
	public String driverSelection(String dbmsName){
		String driverString = "";
		switch (dbmsName) {
		case "postgresql":
			driverString = "jdbc:postgresql://";
			break;
		case "mysql":
			driverString = "jdbc:mysql://";
			break;
		case "oracle":
			driverString = "jdbc:oracle:thin:@";
			break;
		}
		return driverString;
	}
	
	public static Connection estabslishSimpleConnection(String dbmsName, String url) throws SQLException{
		return DriverManager.getConnection(dbmsName+url);
	}
	
	public static Connection establishConnection(String dbmsName, String url, String user, String pw) throws SQLException{
		return DriverManager.getConnection(dbmsName+url,user,pw);
	}
	
	public Connection getConnection(){
		return connection;
	}
}

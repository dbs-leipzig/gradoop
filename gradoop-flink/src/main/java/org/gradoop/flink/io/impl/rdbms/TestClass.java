package org.gradoop.flink.io.impl.rdbms;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gradoop.flink.io.impl.rdbms.jdbcdriver.DriverShim;


public class TestClass {
	public static void main(String[] args) throws SQLException, MalformedURLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
//		URL u = new URL("jar:file:/home/pc/01 Uni/8. Semester/Bachelorarbeit/"
//				+ "gradoop/gradoop-flink/src/main/java/org/gradoop/flink/io/impl/rdbms/jdbcdriverjars/"
//				+ "mysql-connector-java-5.1.46.jar!/");
		String path = new File(System.getProperty("user.dir")).getAbsolutePath().replaceAll("/gradoop-example|/gradoop-flink", "");
		System.out.println(path);
		URL u = new URL("jar:file:" 
				+ System.getProperty("user.dir")
				+ "/src/main/java/org/gr/jdbcdriverjars/"
				+ "postgresql-42.2.2.jar!/");
		String classname = "org.postgresql.Driver";
		URLClassLoader ucl = new URLClassLoader(new URL[] { u });
		Driver driver = (Driver)Class.forName(classname, true, ucl).newInstance();
		DriverManager.registerDriver(new DriverShim(driver));
		Connection con = DriverManager.getConnection("jdbc:postgresql://localhost/cycleTest", "hr73vexy", "UrlaubsReisen");
		getTestConnection(con);
	}
	
	public static void getTestConnection(Connection con) throws SQLException{
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select * from person");
		while(rs.next()){
			System.out.println(rs.getInt(1)+ "|" + rs.getString(2));
		}
		
	}
}

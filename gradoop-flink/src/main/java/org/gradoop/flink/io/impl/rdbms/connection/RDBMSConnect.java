/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;

/**
 * Connection to relational datbase
 */
public class RDBMSConnect {
	
	/**
	 * Establishes a connection to a relational database via jdbc.
	 * 
	 * @return Valid connection to a relational database
	 */
	public static Connection connect(RDBMSConfig config) {
		Connection connection = null;
		try {
			new RegisterDriver().register(config);
			connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPw());
			System.out.println("Successfully connected to " + config.getUrl().replaceAll(".*/", ""));
		} catch (SQLException e) {
			System.err.println("Not possible to establish database connection to " + config.getUrl());
			e.printStackTrace();
		}
		return connection;
	}
}

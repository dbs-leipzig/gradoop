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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Register jdbc driver
 */
public class RegisterDriver {
	
	/**
	 * Registers a jdbc driver 
	 * @param config Valid RDBMS configuration
	 */
	public static void register(RdbmsConfig config) {
		
		try {
			URL driverUrl = new URL("jar:file:" + config.getJdbcDriverPath() + "!/");
			URLClassLoader ucl = new URLClassLoader(new URL[] { driverUrl });
			Driver driver = (Driver) Class.forName(config.getJdbcDriverClassName(), true, ucl).newInstance();
			DriverManager.registerDriver(new DriverShim(driver));

		} catch (SQLException | MalformedURLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			System.err.println("Not possible to establish database connection to DBUrl " + config.getUrl());
			e.printStackTrace();
		}
	}
}

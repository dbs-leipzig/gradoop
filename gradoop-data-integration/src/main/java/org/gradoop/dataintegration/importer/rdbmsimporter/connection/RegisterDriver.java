/*
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
package org.gradoop.dataintegration.importer.rdbmsimporter.connection;

import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Registers a JDBC driver.
 */
public class RegisterDriver {

  /**
   * Object variable of calss {@link RegisterDriver}.
   */
  private static RegisterDriver OBJ = null;

  /**
   * Singleton instance of class {@link RegisterDriver} to registers a driver by given .jar file.
   */
  private RegisterDriver() { }

  /**
   * Creates a single instance of class {@link RegisterDriver}
   *
   * @return single instance of class {@link RegisterDriver}
   */
  public static RegisterDriver create() {
    if (OBJ == null) {
      OBJ = new RegisterDriver();
    }
    return OBJ;
  }


  /**
   * Registers a JDBC driver by a given .jar file.
   *
   * @param config database configuration
   */
  public void register(RdbmsConfig config) {
    AccessController.doPrivileged(new PrivilegedAction<Object>() {

      @Override
      public Object run() {
        Logger logger = Logger.getLogger(RegisterDriver.class);

        try {
          URL driverUrl = new URL("jar:file:" + config.getJdbcDriverPath() + "!/");
          URLClassLoader classLoader = new URLClassLoader(new URL[]{driverUrl});
          Driver driver = (Driver) Class.forName(
            config.getJdbcDriverClassName(), true, classLoader)
            .getDeclaredConstructor().newInstance();
          DriverManager.registerDriver(new GradoopJDBCDriver(driver));

        } catch (SQLException e) {
          System.err.println("Cannot register jdbc driver !");
          logger.error(e);
        } catch (MalformedURLException e) {
          System.err.println("Wrong path to jdbc driver !");
          logger.error(e);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException |
            IllegalArgumentException | InvocationTargetException | NoSuchMethodException |
            SecurityException e) {
          logger.error(e);
        }
        return this;
      }
    });
  }
}

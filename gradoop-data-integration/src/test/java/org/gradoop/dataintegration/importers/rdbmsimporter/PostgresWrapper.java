/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importers.rdbmsimporter;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

/**
 * Wrapper around the PostgreSQL database.
 */
public class PostgresWrapper {

  /**
   * The embedded postgresql wrapper.
   */
  private EmbeddedPostgres embeddedPostgres;

  /**
   * The URL to connect on
   */
  private String connectionUrl;

  /**
   * Start PostgreSQL running
   *
   * @throws IOException if an error occurs starting Postgres
   */
  public void start() throws IOException {
    if (embeddedPostgres == null) {
      int port = getFreePort();
      embeddedPostgres = new EmbeddedPostgres();
      connectionUrl = embeddedPostgres.start("localhost", port, "dbName", "userName", "password");
      embeddedPostgres.getProcess().get().importFromFile(new File(RdbmsDataImportTest.class
        .getResource("/data/rdbms/input/husband.sql").getFile()));
    }
  }

  /**
   * Stop PostgreSQL
   */
  public void stop() {
    if (embeddedPostgres != null) {
      embeddedPostgres.stop();
      embeddedPostgres = null;
    }
  }

  /**
   * Get the URL to use to connect to the database
   *
   * @return the connection URL
   */
  public String getConnectionUrl() {
    return connectionUrl;
  }

  /**
   * Get a free port to listen on
   *
   * @return the port
   * @throws IOException if an error occurs finding a port
   */
  private static int getFreePort() throws IOException {
    @SuppressWarnings("resource")
    ServerSocket s = new ServerSocket(0);
    return s.getLocalPort();
  }
}

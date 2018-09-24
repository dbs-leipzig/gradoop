package org.gradoop.flink.io.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

/**
 * Wrapper around the PostgreSQL database
 */
public class PostgresWrapper {

  /** The embedded postgres wrapper */
  private EmbeddedPostgres embeddedPostgres;
  /** The URL to connect on */
  private String connectionUrl;

  /**
   * Start PostgreSQL running
   * 
   * @throws IOException
   *           if an error occurs starting Postgres
   */
  public void start() throws IOException {
    if (embeddedPostgres == null) {
      int port = getFreePort();
      embeddedPostgres = new EmbeddedPostgres();
      connectionUrl = embeddedPostgres.start("localhost", port, "dbName", "userName", "password");
      embeddedPostgres.getProcess().get().importFromFile(new File(RdbmsDataImportTest.class
          .getResource("/data/rdbms/input/employees_small.sql").getFile()));
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
   * @throws IOException
   *           if an error occurs finding a port
   */
  private static int getFreePort() throws IOException {
    @SuppressWarnings("resource")
    ServerSocket s = new ServerSocket(0);
    return s.getLocalPort();
  }
}

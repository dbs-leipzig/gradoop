package org.gradoop.flink.io.impl.rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class RdbmsDataImportTest extends GradoopFlinkTestBase {

  @Test
  public void testReadHusband() throws Exception {
    String gdlPath = RdbmsDataImportTest.class.getResource("/data/rdbms/expected/cycleTest.gdl")
        .getFile();

    // creates embedded, temporary postgresql database
    PostgresWrapper pw = new PostgresWrapper();
    pw.start();
    Connection con = DriverManager.getConnection(pw.getConnectionUrl());
    con.createStatement().execute("CREATE TABLE person(" + " pnr INT PRIMARY KEY," +
        " name VARCHAR(128)," + " gatte INT," + " FOREIGN KEY (gatte) REFERENCES person(pnr));");
    con.createStatement().execute("INSERT INTO person (pnr,name) VALUES (0,'Peter');");
    con.createStatement().execute("INSERT INTO person VALUES (1,'Karla',0);");
    con.createStatement().execute("INSERT INTO person (pnr,name) VALUES (2,'Joachim');");
    con.createStatement().execute("INSERT INTO person VALUES (3,'Steffen',2);");
    con.createStatement().execute("INSERT INTO person (pnr,name) VALUES (4,'Michael');");
    con.createStatement().execute("INSERT INTO person VALUES (5,'Sven',4);");
    con.createStatement().execute("UPDATE person SET gatte = 1 WHERE pnr = 0;");
    con.createStatement().execute("UPDATE person SET gatte = 3 WHERE pnr = 2;");
    con.createStatement().execute("UPDATE person SET gatte = 5 WHERE pnr = 4;");

    String url = pw.getConnectionUrl();
    String user = "userName";
    String password = "password";
    String jdbcDriverPath = RdbmsDataImportTest.class
        .getResource("/data/rdbms/jdbcDrivers/postgresql-42.2.2.jar").getFile();
    System.out.println(jdbcDriverPath);
    String jdbcDriverClassName = "org.postgresql.Driver";

    // creates rdbms data import of embedded databse
    DataSource dataSource = new RdbmsDataSource(url, user, password, jdbcDriverPath,
        jdbcDriverClassName, getConfig());

    LogicalGraph input = dataSource.getLogicalGraph();
    LogicalGraph expected = getLoaderFromFile(gdlPath).getLogicalGraphByVariable("expected");

    collectAndAssertTrue(input.equalsByElementData(expected));

    // Collection<Vertex> vertices = input.getVertices().collect();
    // Collection<Edge> edges = input.getEdges().collect();

    // assertEquals("Wrong vertice count !",6,vertices.size());
    // assertEquals("Wrong edge count !",6,edges.size());
  }

   @Test
   public void temporaryVerticesTest() {
   DataSet<Vertex> temporaryVertices =
   org.gradoop.flink.io.impl.rdbms.functions.CreateVertices.create(getConfig(),
   rdbmsConfig, tablesToNodes);
   }
}

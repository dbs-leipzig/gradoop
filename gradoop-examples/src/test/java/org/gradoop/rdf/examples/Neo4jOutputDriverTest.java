package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
import org.gradoop.drivers.BulkLoadDriver;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.utils.ConfigurationUtils;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for Neo4jOutputDriver
 */
public class Neo4jOutputDriverTest extends GradoopClusterTest {

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();

    createTestData(conf);
    writeNeo4jOutput(conf);

    // TODO test result
  }

  private void createTestData(Configuration conf) throws Exception {
    String graphFile = "countries.graph";

    String[] args = new String[] {
      "-" + BulkLoadDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
      RDFReader.class.getCanonicalName(),
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/neo4j-output-driver-test",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(graphFile);

    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);
    int bulkExitCode = bulkLoadDriver.run(args);
    assertThat(bulkExitCode, is(0));
  }

  private void writeNeo4jOutput(Configuration conf) throws Exception {
    String[] args = new String[]{
      "-" + Neo4jOutputDriver.ConfUtils.OPTION_NEO4J_OUTPUT_PATH, ""};

    Neo4jOutputDriver driver = new Neo4jOutputDriver();
    driver.setConf(conf);
    int neoExitCode = driver.run(args);
    assertThat(neoExitCode, is(0));

    // todo read output and validate:q
  }
}

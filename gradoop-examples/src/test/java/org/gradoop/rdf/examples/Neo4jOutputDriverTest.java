package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
import org.gradoop.drivers.BulkLoadDriver;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.storage.GraphStore;
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
    createTestData();
    writeNeo4jOutput();

    // read output again!?
  }

  private void createTestData() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "61_merged_files.graph";

    String[] args = new String[] {
      "-" + BulkLoadDriver.ConfUtils.OPTION_VERTEX_LINE_READER,
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

    String[] argsEnrich = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/neo4j-mapreduce",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    RDFInstanceEnrichmentDriver enrichDrv = new RDFInstanceEnrichmentDriver();
    enrichDrv.setConf(conf);
    int enrichExitCode = enrichDrv.run(argsEnrich);
    assertThat(enrichExitCode, is(0));
  }

  private void writeNeo4jOutput() throws Exception {
    GraphStore graphStore = openGraphStore();

    String[] args = new String[]{
      "-" + Neo4jOutputDriver.ConfUtils.OPTION_NEO4J_OUTPUT_PATH, ""};

    Neo4jOutputDriver driver = new Neo4jOutputDriver(args);
    driver.setGraphStore(graphStore);
    driver.writeOutput();
  }
}

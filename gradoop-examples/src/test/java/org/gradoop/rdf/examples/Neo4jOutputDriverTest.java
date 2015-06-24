package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
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

    // we need the RDFAnalysis - Neo4jOutputDriver needs the graph table in
    // HBase
    computeRDFAnalysis(conf);
    writeNeo4jOutput(conf);
  }

  private void computeRDFAnalysis(Configuration conf) throws Exception {
    String graphFile = "countries.graph";

    String[] args = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_REDUCERS, "1",
      "-" + ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/neo4j-output-driver-test",
      "-" + ConfigurationUtils.OPTION_TABLE_PREFIX, "neoTest",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(graphFile);
    RDFAnalysisDriver rdfAnalysisDriver = new RDFAnalysisDriver();
    rdfAnalysisDriver.setConf(conf);

    // run the pipeline
    int exitCode = rdfAnalysisDriver.run(args);
    assertThat(exitCode, is(0));

  }

  private void writeNeo4jOutput(Configuration conf) throws Exception {
    String[] args = new String[]{
      "-" + Neo4jOutputDriver.ConfUtils.OPTION_NEO4J_OUTPUT_PATH, "",
      "-" + ConfigurationUtils.OPTION_TABLE_PREFIX, "neoTest"
    };

    Neo4jOutputDriver driver = new Neo4jOutputDriver();
    driver.setConf(conf);
    int neoExitCode = driver.run(args);
    assertThat(neoExitCode, is(0));
  }
}

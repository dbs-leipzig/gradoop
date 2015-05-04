package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.GradoopClusterTest;
import org.gradoop.drivers.BulkLoadDriver;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Simple pipeline test for
 * {@link org.gradoop.rdf.examples.RDFInstanceEnrichmentDriver}.
 */
public class RDFInstanceEnrichmentDriverTest extends GradoopClusterTest {
  private static final Logger LOG =
    Logger.getLogger(RDFInstanceEnrichmentDriverTest.class);
  private static final String[] LABELS = new String[]{"rdfs:label",
                                                      "skos:prefLabel",
                                                      "gn:name",
                                                      "empty_property",
                                                      "no_property"};
  private static final String DB_PATH = "output/neo4j-db";
  private static final String VERTICES_PREFIX = "enrich";


  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "countries.graph";

    createTestData(conf, graphFile);
    enrichData(conf);
    testData(conf);
  }

  private void testData(Configuration conf) throws Exception {
    GraphStore graphStore = openGraphStore();
    String[] args =
      new String[]{"-" + Neo4jOutputDriver.ConfUtils.OPTION_NEO4J_OUTPUT_PATH,
                   DB_PATH};

    Neo4jOutputDriver driver = new Neo4jOutputDriver();
    driver.setConf(conf);
    int testExitCode = driver.run(args);
    assertThat(testExitCode, is(0));

    String tableName = VERTICES_PREFIX + GConstants.DEFAULT_TABLE_VERTICES;
    Iterator<Vertex> vertices = graphStore.getVertices(tableName);
    while (vertices.hasNext()) {
      Vertex vertex = vertices.next();
      for (String s : vertex.getPropertyKeys()) {
        boolean isProperty = false;

        for (String label : LABELS) {
          if (s.contains(label)) {
            isProperty = true;
            break;
          }
        }
        LOG.info("===vertex: " + vertex.getLabels().iterator().next() +
          " key: " + s + " value: " + vertex.getProperty(s));
        assertTrue(isProperty);
      }
    }
    graphStore.close();
  }

  private void enrichData(Configuration conf) throws Exception {
    String[] args = new String[]{"-" + ConfigurationUtils.OPTION_TABLE_PREFIX,
                   VERTICES_PREFIX};

    RDFInstanceEnrichmentDriver enrichDrv = new RDFInstanceEnrichmentDriver();
    enrichDrv.setConf(conf);
    int enrichExitCode = enrichDrv.run(args);
    assertThat(enrichExitCode, is(0));
  }

  private void createTestData(Configuration conf, String graphFile) throws
    Exception {
    String[] argsBulk = new String[] {
      "-" + BulkLoadDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
      RDFReader.class.getCanonicalName(),
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/rdf-enrich",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES,
      "-" + ConfigurationUtils.OPTION_TABLE_PREFIX, VERTICES_PREFIX
    };

    copyFromLocal(graphFile);

    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);
    int bulkExitCode = bulkLoadDriver.run(argsBulk);
    assertThat(bulkExitCode, is(0));
  }
}

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
  private static final String VERTICES_PREFIX = "enrich";
  private static final String GRAPH_FILE = "countries.graph";

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();

    createTestData(conf);
    enrichData(conf);
    testData();
  }

  private void testData() throws Exception {
    GraphStore graphStore = openGraphStore();
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

  private void createTestData(Configuration conf) throws Exception {
    String[] argsBulk = new String[] {
      "-" + BulkLoadDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
      RDFReader.class.getCanonicalName(),
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, GRAPH_FILE,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/rdf-enrich",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES,
      "-" + ConfigurationUtils.OPTION_TABLE_PREFIX, VERTICES_PREFIX
    };
    copyFromLocal(GRAPH_FILE);
    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);

    int bulkExitCode = bulkLoadDriver.run(argsBulk);

    assertThat(bulkExitCode, is(0));
  }
}

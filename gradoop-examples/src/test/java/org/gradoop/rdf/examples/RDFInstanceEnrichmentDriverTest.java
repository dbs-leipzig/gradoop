package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.drivers.BulkLoadDriver;
import org.gradoop.io.reader.RDFReader;
import org.gradoop.io.writer.Neo4jLineWriter;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.FileNotFoundException;

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
                                                      "empty_property"};
  private static final String DB_PATH = "output/neo4j-db";


  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "countries.graph";

    createTestData(conf, graphFile);
    enrichData(conf, graphFile);
    testData();
  }

  private void testData() throws FileNotFoundException {
    GraphStore graphStore = openGraphStore();
    Neo4jLineWriter writer = new Neo4jLineWriter(DB_PATH);
    writer.produceOutput(graphStore);
    writer.shutdown();

    for (Vertex vertex : graphStore.readVertices()) {
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

  private void enrichData(Configuration conf, String graphFile) throws
    Exception {
    String[] argsEnrich = new String[] {
      "-" + ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500",
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/rdf-enrich",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    RDFInstanceEnrichmentDriver enrichDrv = new RDFInstanceEnrichmentDriver();
    enrichDrv.setConf(conf);
    int enrichExitCode = enrichDrv.run(argsEnrich);
    assertThat(enrichExitCode, is(0));
  }

  private void createTestData(Configuration conf, String graphFile) throws
    Exception {
    String[] argsBulk = new String[] {
      "-" + BulkLoadDriver.ConfUtils.OPTION_VERTEX_LINE_READER,
      RDFReader.class.getCanonicalName(),
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH,
      "/output/import/rdf-enrich",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(graphFile);

    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);
    int bulkExitCode = bulkLoadDriver.run(argsBulk);
    assertThat(bulkExitCode, is(0));
  }
}

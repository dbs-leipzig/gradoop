package org.gradoop.drivers;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
import org.gradoop.io.reader.JsonReader;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link org.gradoop.drivers.BulkLoadDriver}.
 */
public class BulkLoadDriverTest extends GradoopClusterTest {
  @Test
  public void testBulkLoadDriver() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "json-sample.graph";
    String[] args =
      new String[]{"-" + BulkLoadDriver.OPTION_GRAPH_INPUT_PATH, graphFile,
                   "-" + BulkLoadDriver.OPTION_GRAPH_OUTPUT_PATH,
                   "/output/import/extended-graph",
                   "-" + BulkLoadDriver.OPTION_CUSTOM_ARGUMENT,
                   "sna-reader.label=knows, sna-reader" +
                     ".meta_data=person_knows_person.meta",
                   "-" + BulkLoadDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
                   JsonReader.class.getCanonicalName(),
                   "-" + BulkLoadDriver.LoadConfUtils.OPTION_DROP_TABLES};
    copyFromLocal(graphFile);
    BulkLoadDriver bulkLoadDriver = new BulkLoadDriver();
    bulkLoadDriver.setConf(conf);
    // run the bulk load
    int exitCode = bulkLoadDriver.run(args);
    // testing
    assertThat(exitCode, is(0));
    validateExtendedGraphVertices(openGraphStore());
  }
}

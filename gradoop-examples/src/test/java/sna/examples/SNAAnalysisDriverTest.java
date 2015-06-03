package sna.examples;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.model.Vertex;
import org.gradoop.sna.examples.SNAAnalysisDriver;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.net.URL;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Tests the pipeline described in
 * {@link SNAAnalysisDriver}.
 */
public class SNAAnalysisDriverTest extends GradoopClusterTest {

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String nodeFile = "person.csv";
    String edgeFile = "person_knows_person.csv";
    URL resourceUrl = getClass().getResource("/person_meta.csv");
    String[] args = new String[] {
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_DROP_TABLES,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_BULKLOAD,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
      "CSVReader", "-" + SNAAnalysisDriver.OPTION_GRAPH_INPUT_PATH, "",
      "-" + SNAAnalysisDriver.OPTION_GRAPH_OUTPUT_PATH, "/output/sna",
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_METADATA_PATH,
      resourceUrl.getPath().replace("person_meta.csv", ""),
      "-" + SNAAnalysisDriver.OPTION_VERBOSE,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_LABLEPROPAGATION,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_WORKERS, "1",
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_SUMMARIZE,
      "/output/summarize"
    };
    copyFromLocal(nodeFile);
    copyFromLocal(edgeFile);
    SNAAnalysisDriver snaAnalysisDriver = new SNAAnalysisDriver();
    snaAnalysisDriver.setConf(conf);
    //run the pipeline
    int exitCode = snaAnalysisDriver.run(args);
    //test
    assertThat(exitCode, is(0));
    GraphStore graphStore = openGraphStore();
    // Label Propagation Result
    validateLabelPropagation(graphStore);
    graphStore.close();
  }

  private void validateLabelPropagation(GraphStore graphStore) {
    validateCommunities(graphStore.readVertex(0L), 0L);
    validateCommunities(graphStore.readVertex(1L), 0L);
    validateCommunities(graphStore.readVertex(2L), 0L);
    validateCommunities(graphStore.readVertex(3L), 0L);
    validateCommunities(graphStore.readVertex(4L), 4L);
    validateCommunities(graphStore.readVertex(5L), 4L);
    validateCommunities(graphStore.readVertex(6L), 4L);
    validateCommunities(graphStore.readVertex(7L), 4L);
    validateProperties(graphStore.readVertex(0L), "Pham");
    validateProperties(graphStore.readVertex(1L), "Aquino");
    validateProperties(graphStore.readVertex(2L), "Richter");
    validateProperties(graphStore.readVertex(3L), "Kumar");
    validateProperties(graphStore.readVertex(4L), "Li");
    validateProperties(graphStore.readVertex(5L), "Tembo");
    validateProperties(graphStore.readVertex(6L), "Hayvoronsky");
    validateProperties(graphStore.readVertex(7L), "Kumar");
  }

  private void validateProperties(Vertex vertex, String lastName) {
    assertEquals(vertex.getProperty("lastName"), lastName);
  }

  private void validateCommunities(Vertex vertex, long... expectedCommunities) {
    assertEquals(expectedCommunities.length, vertex.getGraphCount());
    List<Long> graphIDs = Lists.newArrayList(vertex.getGraphs());
    for (long expectedCommunity : expectedCommunities) {
      assertTrue(graphIDs.contains(expectedCommunity));
    }
  }
}
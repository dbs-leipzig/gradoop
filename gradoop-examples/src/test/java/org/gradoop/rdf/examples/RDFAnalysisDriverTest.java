package org.gradoop.rdf.examples;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GConstants;
import org.gradoop.GradoopClusterTest;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Tests the pipeline described in
 * {@link org.gradoop.rdf.examples.RDFAnalysisDriver}.
 */
public class RDFAnalysisDriverTest extends GradoopClusterTest {
  private static final long MAX_GRAPHS1 = -9022373811030210963L;
  private static final long MAX_GRAPHS2 = -7402558968462893373L;
  private static final long MAX_GRAPHS3 = -8374894514776459130L;

  private static final int VERTICES_COUNT_12 = 12;
  private static final int VERTICES_COUNT_11 = 11;

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "factNyt-noSpoken.graph";
    String tablePrefix = "rdfAnalysis";

    String[] args = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_REDUCERS, "1",
      "-" + ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH, "/output/import/rdf",
      "-" + ConfigurationUtils.OPTION_TABLE_PREFIX, tablePrefix,
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(graphFile);
    RDFAnalysisDriver rdfAnalysisDriver = new RDFAnalysisDriver();
    rdfAnalysisDriver.setConf(conf);
    // run the pipeline
    int exitCode = rdfAnalysisDriver.run(args);

    // tests
    assertThat(exitCode, is(0));
    GraphStore graphStore = openGraphStore(tablePrefix);
    HashSet<Graph> graphs = getGraphs(graphStore, tablePrefix);
    validateSelectAndAggregate(graphs);

    graphStore.close();
  }

  private HashSet<Graph> getGraphs(GraphStore graphStore, String tablePrefix)
    throws Exception {
    String tableName = tablePrefix + GConstants.DEFAULT_TABLE_VERTICES;

    Iterator<Vertex> vertices = graphStore.getVertices(tableName);

    HashSet<Graph> graphs = new HashSet<>();
    while (vertices.hasNext()) {
      Long graphId = vertices.next().getGraphs().iterator().next();
      Graph graph = graphStore.readGraph(getComponent(graphStore, graphId));
      graphs.add(graph);
    }

    return graphs;
  }

  private void validateSelectAndAggregate(HashSet<Graph> graphs) throws
    Exception {
    assertEquals(18584, graphs.size());

    for (Graph g : graphs) {
      assertNotNull(g);
      assertEquals(1, g.getPropertyCount());
      int count = (int)
        g.getProperty(SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);
      assertNotNull(count);
      
      if (g.getID() == MAX_GRAPHS1) {
        assertEquals(VERTICES_COUNT_12, count);
      }
      if (g.getID() == MAX_GRAPHS2) {
        assertEquals(VERTICES_COUNT_11, count);
      }
      if (g.getID() == MAX_GRAPHS3) {
        assertEquals(VERTICES_COUNT_11, count);
      }
    }
  }

  private Long getComponent(GraphStore graphStore, Long vID) {
    return graphStore.readVertex(vID).getGraphs().iterator().next();

  }
}

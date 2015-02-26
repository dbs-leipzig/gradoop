package org.gradoop.biiig.examples;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.biiig.utils.ConfigurationUtils;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Tests the pipeline described in {@link RDFAnalysisDriver}.
 */
public class RDFAnalysisDriverTest extends GradoopClusterTest {
  private static final Long A_DBP = -4820974574369803382L;
  private static final Long A_FAC = -3558936620827054041L;
  private static final Long A_GEO = -3728619970397603446L;
  private static final Long A_NYT = -2056322834497809177L;
  private static final Long A_LGD = 85441770852151607L;
  private static final Long D_DBP = 8473659083696230591L;
  private static final Long D_FAC = -9198211274455686300L;
  private static final Long D_GEO = 7366010066576370289L;
  private static final Long D_NYT = 3410771805009961568L;
  private static final Long D_LGD = 1506527894713967875L;
  private static final Long G_DBP = -1005574797037940634L;
  private static final Long G_FAC = 8710406901643533074L;
  private static final Long G_GEO = 8152360427669492775L;
  private static final Long G_NYT = -552704442989841987L;
  private static final Long G_LGD = -3957714962320320763L;

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "countries.graph";

    String[] args = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_REDUCERS, "1",
      "-" + ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, graphFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH, "/output/import/rdf",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(graphFile);
    RDFAnalysisDriver rdfAnalysisDriver = new RDFAnalysisDriver();
    rdfAnalysisDriver.setConf(conf);

    // run the pipeline
    int exitCode = rdfAnalysisDriver.run(args);

    // tests
    assertThat(exitCode, is(0));
    GraphStore graphStore = openGraphStore();
    // RDF results
    validateComponentForSampleVertex(graphStore);
    validateSelectAndAggregate(graphStore);

    graphStore.close();
  }

  private void validateComponentForSampleVertex(GraphStore graphStore) {
    Long aRef = getComponent(graphStore, A_DBP);
    Long dRef = getComponent(graphStore, D_DBP);
    Long gRef = getComponent(graphStore, G_DBP);

    validateRDF(graphStore.readVertex(A_DBP), aRef);
    validateRDF(graphStore.readVertex(A_FAC), aRef);
    validateRDF(graphStore.readVertex(A_GEO), aRef);
    validateRDF(graphStore.readVertex(A_NYT), aRef);
    validateRDF(graphStore.readVertex(A_LGD), aRef);

    validateRDF(graphStore.readVertex(D_DBP), dRef);
    validateRDF(graphStore.readVertex(D_FAC), dRef);
    validateRDF(graphStore.readVertex(D_GEO), dRef);
    validateRDF(graphStore.readVertex(D_NYT), dRef);
    validateRDF(graphStore.readVertex(D_LGD), dRef);

    validateRDF(graphStore.readVertex(G_DBP), gRef);
    validateRDF(graphStore.readVertex(G_FAC), gRef);
    validateRDF(graphStore.readVertex(G_GEO), gRef);
    validateRDF(graphStore.readVertex(G_NYT), gRef);
    validateRDF(graphStore.readVertex(G_LGD), gRef);
  }

  private void validateSelectAndAggregate(GraphStore graphStore) {
    Graph afghanistan = graphStore.readGraph(getComponent(graphStore, A_DBP));
    Graph germany = graphStore.readGraph(getComponent(graphStore, D_DBP));
    Graph ghana = graphStore.readGraph(getComponent(graphStore, G_DBP));

    List<Graph> graphs = Lists.newArrayListWithCapacity(3);
    graphs.add(afghanistan);
    graphs.add(germany);
    graphs.add(ghana);

    for (Graph g : graphs) {
      assertNotNull(g);
      assertEquals(1, g.getPropertyCount());
      Object count =
        g.getProperty(SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);
//      LOG.info("============= g: " + g.getID().toString());
//      LOG.info("============= count value: " + count);
      assertNotNull(count);
      // each graph has 5 vertices which are connected
      assertEquals(5, count);
    }
  }

  private void validateRDF(Vertex vertex, long reference) {
    assertEquals(vertex.getGraphCount(), 1);

    assertThat(vertex.getGraphs().iterator().next(), is(reference));
  }

  private Long getComponent(GraphStore graphStore, Long vID) {
    return graphStore.readVertex(vID).getGraphs().iterator().next();

  }
}

package org.gradoop.rdf.examples;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.drivers.BulkWriteDriver;
import org.gradoop.io.reader.JsonReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.io.writer.JsonWriter;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Tests the pipeline described in {@link org.gradoop.rdf.examples.RDFAnalysisDriver}.
 */
public class RDFAnalysisDriverTest extends GradoopClusterTest {
  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(RDFAnalysisDriverTest.class);
  private static final Long A_DBP = -4820974574369803382L;
  private static final Long A_FAC = -3558936620827054041L;
//  private static final Long A_GEO = -3728619970397603446L;
  private static final Long A_NYT = -2056322834497809177L;
//  private static final Long A_LGD = 85441770852151607L;

  private static final Long D_DBP =  8473659083696230591L;
  private static final Long D_FAC = -9198211274455686300L;
//  private static final Long D_GEO =  7366010066576370289L;
  private static final Long D_NYT =  3410771805009961568L;
//  private static final Long D_LGD =  1506527894713967875L;

  private static final Long G_DBP = -1005574797037940634L;
  private static final Long G_FAC = 8710406901643533074L;
//  private static final Long G_GEO = 8152360427669492775L;
  private static final Long G_NYT = -552704442989841987L;
//  private static final Long G_LGD = -3957714962320320763L;

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();
    String graphFile = "factNyt-noSpoken.graph";

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

  private HashSet<Long> getUniqueComponentIDs() throws Exception {
    String outputDirName = "/output/export/rdf";

    String[] outArgs = new String[] {
      "-" + BulkWriteDriver.ConfUtils.OPTION_VERTEX_LINE_WRITER,
      JsonWriter.class.getName(),
      "-" + BulkWriteDriver.ConfUtils.OPTION_HBASE_SCAN_CACHE, "10",
      "-" + BulkWriteDriver.ConfUtils.OPTION_GRAPH_OUTPUT_PATH, outputDirName
    };

    BulkWriteDriver bulkWriteDriver = new BulkWriteDriver();
    bulkWriteDriver.setConf(utility.getConfiguration());

    // run the bulk write
    int outExitCode = bulkWriteDriver.run(outArgs);

    // testing
    assertThat(outExitCode, CoreMatchers.is(0));

    // read map output
    int count = 18584;
    Path outputFile = new Path(outputDirName, "part-m-00000");
    String[] fileContent = readGraphFromFile(outputFile, count);

    HashSet<Long> uniqueComponents = new HashSet<>();
    VertexLineReader reader = new JsonReader();
    for (String line : fileContent) {
      if (!line.isEmpty()) {
        Vertex vertex = reader.readVertex(line);
        Long graphID = vertex.getGraphs().iterator().next();
        if (graphID == -9022373811030210963L) {
          LOG.info("======== max value vertex: " + vertex.toString());
        }
        if (graphID == -7402558968462893373L) {
          LOG.info("======== -7402558968462893373 vertex: " + vertex.toString());
        }
        if (graphID == -8374894514776459130L) {
          LOG.info("======== -8374894514776459130 vertex: " + vertex.toString());
        }
        uniqueComponents.add(graphID);
      }
    }
    return uniqueComponents;
  }

  private void validateComponentForSampleVertex(GraphStore graphStore) {
    Long aRef = getComponent(graphStore, A_DBP);
    Long dRef = getComponent(graphStore, D_DBP);
    Long gRef = getComponent(graphStore, G_DBP);

    validateRDF(graphStore.readVertex(A_DBP), aRef);
    validateRDF(graphStore.readVertex(A_FAC), aRef);
    validateRDF(graphStore.readVertex(A_NYT), aRef);
//    validateRDF(graphStore.readVertex(A_LGD), aRef);
//    validateRDF(graphStore.readVertex(A_GEO), aRef);

    validateRDF(graphStore.readVertex(D_DBP), dRef);
    validateRDF(graphStore.readVertex(D_FAC), dRef);
    validateRDF(graphStore.readVertex(D_NYT), dRef);
//    validateRDF(graphStore.readVertex(D_LGD), dRef);
//    validateRDF(graphStore.readVertex(D_GEO), dRef);

    validateRDF(graphStore.readVertex(G_DBP), gRef);
    validateRDF(graphStore.readVertex(G_FAC), gRef);
    validateRDF(graphStore.readVertex(G_NYT), gRef);
//    validateRDF(graphStore.readVertex(G_LGD), gRef);
//    validateRDF(graphStore.readVertex(G_GEO), gRef);
  }

  private void validateSelectAndAggregate(GraphStore graphStore) throws
    Exception {
//    Graph afghanistan = graphStore.readGraph(getComponent(graphStore, A_DBP));
//    Graph germany = graphStore.readGraph(getComponent(graphStore, D_DBP));
//    Graph ghana = graphStore.readGraph(getComponent(graphStore, G_DBP));
//
//    List<Graph> graphs = Lists.newArrayListWithCapacity(3);
//    graphs.add(afghanistan);
//    graphs.add(germany);
//    graphs.add(ghana);


    HashSet<Long> componentIDs = getUniqueComponentIDs();
    List<Graph> graphs = Lists.newArrayListWithCapacity(componentIDs.size());

    for (Long id: componentIDs) {
      graphs.add(graphStore.readGraph(getComponent(graphStore, id)));
    }

    int min = Integer.MAX_VALUE;
    int max = 0;
    Graph minGraph = null;
    Graph maxGraph = null;

    HashMap<Graph, Integer> graphIntegerMap = new HashMap<>();

    for (Graph g : graphs) {
      assertNotNull(g);
      assertEquals(1, g.getPropertyCount());
      Object count =
        g.getProperty(SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY);
      if ((int) count > 8) {
        graphIntegerMap.put(g, (int) count);
      }
      if ((int) count < min) {
        min = (int) count;
        minGraph = g;
      }
      if ((int) count > max) {
        max = (int) count;
        maxGraph = g;
      }
//      LOG.info("===== graph ID: " + g.getID().toString());
//      LOG.info("===== - count value: " + count);
      assertNotNull(count);
      // each graph has 5 vertices which are connected
      //assertEquals(5, count);
    }

    for (Map.Entry<Graph, Integer> entry : graphIntegerMap.entrySet()) {
      LOG.info("===== graphIntMap: key: " + entry.getKey() + " value: " +
        entry.getValue());
    }

    LOG.info("===== number of components with graphs >= 9: " + graphIntegerMap
      .size());
    LOG.info("===== number of components: " + componentIDs.size());
    if (maxGraph != null) {
      LOG.info("===== max count " + max + " found for component: "
        + maxGraph.getID().toString());
    }
    if (minGraph != null) {
      LOG.info("===== min count " + min + " found for component: "
        + minGraph.getID().toString());
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

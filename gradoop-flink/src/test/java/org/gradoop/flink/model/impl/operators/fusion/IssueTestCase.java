package org.gradoop.flink.model.impl.operators.fusion;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphsBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphContainmentFilterBroadcast.GRAPH_ID;

/**
 * Created by vasistas on 21/02/17.
 */
public class IssueTestCase extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getSimpleUsecaseLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/issueTestCase.gdl");
    return getLoaderFromStream(inputStream);
  }

  @Test
  public void with_no_graph_collection() throws Exception {
    FlinkAsciiGraphLoader l = getSimpleUsecaseLoader();
    GraphCollection gc = l.getGraphCollectionByVariables("g0","g1");
    LogicalGraph lg = l.getLogicalGraphByVariable("research");
    LogicalGraph union = new Combination().execute(l.getLogicalGraphByVariable("a1"),l
      .getLogicalGraphByVariable("a2"));


    List<GraphHead> uColl = Lists.newArrayList();
    List<GraphHead> ghColl = Lists.newArrayList();
    List<Vertex> unionVertices = Lists.newArrayList();

    gc.getGraphHeads().output(new LocalCollectionOutputFormat<>(ghColl));
    union.getGraphHead().output(new LocalCollectionOutputFormat<>(uColl));
    union.getVertices().output(new LocalCollectionOutputFormat<>(unionVertices));

    getExecutionEnvironment().execute();

    System.out.println("union GraphHeads' Ids:");
    uColl.forEach(x -> System.out.println(x.getId()));

    System.out.println("GraphCollection GraphHeads' Ids:");
    ghColl.forEach(x -> System.out.println(x.getId()));

    System.out.println("unionVertices' Ids:");
    unionVertices.forEach(x -> System.out.println(x.getGraphIds()));

    DataSet<Vertex> v = gc.getVertices()
        .filter(new InGraphBroadcast<>())
        .withBroadcastSet(lg.getGraphHead().map(new Id<>()),GRAPH_ID);
    //System.out.println(v.count());
    v = gc.getVertices()
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(union.getGraphHead().map(new Id<>()),
        GRAPH_ID);
    System.out.println(v.count());
  }

}

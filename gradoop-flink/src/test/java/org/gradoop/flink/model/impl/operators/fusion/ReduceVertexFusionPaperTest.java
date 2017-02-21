package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.fusion.reduce.ReduceVertexFusion;
import org.gradoop.flink.model.impl.operators.fusion.tuples.SemanticTuple;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Giacomo Bergami on 01/02/17.
 */
public class ReduceVertexFusionPaperTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getBibNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/jointest.gdl");
    return getLoaderFromStream(inputStream);
  }

  public GraphCollection getHyperverticesWithProperties(LogicalGraph left, LogicalGraph right) {
      DataSet<SemanticTuple> conversion = left.getVertices()
        .join(right.getVertices())
        .where((Vertex x)->x.getPropertyValue("name").getString().hashCode())
        .equalTo((Vertex y)->y.getPropertyValue("fstAuth").getString().hashCode())
        .with(new FlatJoinFunction<Vertex, Vertex, SemanticTuple>() {

          private final SemanticTuple reusable = new SemanticTuple();
          private final Edge e = new Edge();
          private final GraphHead gh = new GraphHead();

          @Override
          public void join(Vertex first, Vertex second, Collector<SemanticTuple>
            out) throws
            Exception {
            if (first.getPropertyValue("name").equals(second.getPropertyValue("fstAuth"))) {
              e.setId(GradoopId.get());
              e.setSourceId(first.getId());
              e.setTargetId(second.getId());
              e.setLabel("relation");
              reusable.f3 = e;
              reusable.f0 = first.getId();
              reusable.f1 = second.getId();
              gh.setLabel("fused");
              gh.setProperty("title",second.getPropertyValue("title"));
              gh.setProperty("fstAuth",second.getPropertyValue("fstAuth"));
              gh.setId(GradoopId.get());
              System.out.println("Creating final edges [OK]= " + first.getPropertyValue("name")+":"
                  + first.getId()
                  + "-->"+second.getPropertyValue
                ("title")+":"+second.getId());
              reusable.f2 = gh;
              out.collect(reusable);
            }
          }
        })
        .returns(SemanticTuple.class);
      DataSet<Vertex> finalVertices =
        conversion
          .coGroup(left.getVertices())
          .where((SemanticTuple t)->t.f0).equalTo(new Id<>())
          .with(new CoGroupFunction<SemanticTuple, Vertex, Vertex>() {
            @Override
            public void coGroup(Iterable<SemanticTuple> first, Iterable<Vertex> second,
              Collector<Vertex> out) throws Exception {
              for (Vertex v : second) {
                for (SemanticTuple t : first) {
                  v.addGraphId(t.f2.getId());
                }
                System.out.println("Vertex Left [OK]="+v.toString());
                out.collect(v);
              }
            }
          })
          .returns(Vertex.class)
          .union(conversion.coGroup(right.getVertices())
                  .where((SemanticTuple t)->t.f1).equalTo(new Id<>())
          .with(new CoGroupFunction<SemanticTuple, Vertex, Vertex>() {
            @Override
            public void coGroup(Iterable<SemanticTuple> first, Iterable<Vertex> second,
              Collector<Vertex> out) throws Exception {
              for (Vertex v : second) {
                for (SemanticTuple t : first) {
                  v.addGraphId(t.f2.getId());
                }
                System.out.println("Vertex Right [OK]="+v.toString());
                out.collect(v);
              }
            }
          }).returns(Vertex.class));
      DataSet<Edge> finalEdges =
          conversion
            .map((SemanticTuple t)->t.f3)
            .returns(Edge.class);
      DataSet<GraphHead> finalHeads =
          conversion
            .map((SemanticTuple t)->t.f2)
            .returns(GraphHead.class);
      return GraphCollection.fromDataSets(finalHeads,finalVertices,finalEdges,left.getConfig());
  }

  /**
   * joining empties shall not return errors
   * @throws Exception
   */
  @Test
  public void with_no_graph_collection() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GraphCollection empty = loader.getGraphCollectionByVariables();
    LogicalGraph left = loader.getLogicalGraphByVariable("research");
    LogicalGraph right = loader.getLogicalGraphByVariable("citation");
    ReduceVertexFusion f = new ReduceVertexFusion();
    LogicalGraph output = f.execute(left, right, empty);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * joining empties shall not return errors
   * @throws Exception
   */
  @Test
  public void full_disjunctive_example() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GraphCollection hypervertices = loader.getGraphCollectionByVariables("g0","g1","g2","g3","g4");
    LogicalGraph left = loader.getLogicalGraphByVariable("research");
    LogicalGraph right = loader.getLogicalGraphByVariable("citation");
    ReduceVertexFusion f = new ReduceVertexFusion();
    LogicalGraph output = f.execute(left, right, hypervertices);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
  }



}

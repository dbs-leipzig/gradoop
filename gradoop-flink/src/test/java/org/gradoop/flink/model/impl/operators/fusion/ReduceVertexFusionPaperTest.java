package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
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

/**
 * Created by Giacomo Bergami on 01/02/17.
 */
public class ReduceVertexFusionPaperTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  private static final Function<Vertex, Long> leftHash = v -> (long)v.getPropertyValue("name")
  .getString().hashCode();
  private static final Function<Vertex, Long> rightHash = v -> (long)v.getPropertyValue("fstAuth")
    .getString().hashCode();
  private static final Function<Vertex,Function<Vertex,Boolean>> thetaVertex = x -> y -> x
    .getPropertyValue("name").equals(y.getPropertyValue("fstAuth"));

  private static final String researchGateGraph = "research:RG{}[" +
    "(a1:User {name: \"Alice\"}) " +
    "(a2:User {name: \"Bob\"}) " +
    "(a3:User {name: \"Carl\"}) " +
    "(a4:User {name: \"Dan\"}) " +
    "(a5:User {name: \"Edward\"}) " +
    "(a1)-[:follows]->(a2) " +
    "(a1)-[:follows]->(a3) " +
    "(a2)-[:follows]->(a4) " +
    "(a4)-[:follows]->(a3) " +
    "(a5)-[:follows]->(a1) " +
    "]";

  private static final String citationGraph = "citation:CG{}[" +
    "(p1:Paper {title: \"Graphs\", fstAuth: \"Alice\"}) " +
    "(p2:Paper {title: \"Join\", fstAuth: \"Alice\"}) " +
    "(p3:Paper {title: \"OWL\", fstAuth: \"Bob\"}) " +
    "(p4:Paper {title: \"Projection\", fstAuth: \"Carl\"}) " +
    "(p5:Paper {title: \"muCalc\", fstAuth: \"Dan\"}) " +
    "(p6:Paper {title: \"Matita\", fstAuth: \"Asperti\"}) "+
    "(p1)-[:cites]->(p3) " +
    "(p2)-[:cites]->(p4) " +
    "(p3)-[:cites]->(p4) " +
    "(p4)-[:cites]->(p5) " +
    "(p1)-[:cites]->(p6) " +
    "]";

  private static final String joinedDisjunctively = "result:RGCG{}[" +
    "(r0:User {name: \"Edward\"}) " +
    "(r1:fused {title: \"Graphs\", fstAuth: \"Alice\"}) " +
    "(r2:fused {title: \"Join\", fstAuth: \"Alice\"}) " +
    "(r3:fused {title: \"OWL\", fstAuth: \"Bob\"}) " +
    "(r4:fused {title: \"Projection\", fstAuth: \"Carl\"}) " +
    "(r5:fused {title: \"muCalc\", fstAuth: \"Dan\"}) " +
    "(r6:Paper {title: \"Matita\", fstAuth: \"Asperti\"}) " +
    "(r0)-[:follows]->(r1) " +
    "(r0)-[:follows]->(r2) " +
    "(r1)-[:follows]->(r3) " +
    "(r2)-[:follows]->(r4) " +
    "(r1)-[:follows]->(r3) " +
    "(r2)-[:follows]->(r4) " +
    "(r3)-[:cites]->(r4) " +
    "(r4)-[:cites]->(r5) " +
    "(r1)-[:follows]->(r4) " +
    "(r2)-[:follows]->(r3) " +
    "(r3)-[:follows]->(r5) " +
    "(r5)-[:follows]->(r4) " +
    "(r1)-[:cites]->(r6) " +
    "]";

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
  public void full_disjunctive_example() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(researchGateGraph+" "+citationGraph+" "+
      joinedDisjunctively);
    LogicalGraph left = loader.getLogicalGraphByVariable("research");
    LogicalGraph right = loader.getLogicalGraphByVariable("citation");

    ReduceVertexFusion f = new ReduceVertexFusion();
    LogicalGraph output = f.execute(left, right, getHyperverticesWithProperties
      (left,right));
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
  }



}

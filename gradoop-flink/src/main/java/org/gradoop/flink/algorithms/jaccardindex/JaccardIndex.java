package org.gradoop.flink.algorithms.jaccardindex;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.functions.ComputeScores;
import org.gradoop.flink.algorithms.jaccardindex.functions.ConfigurableEdgeKeySelector;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupPairs;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupSpans;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroups;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.IncomingVertexDegrees;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction.INDEGREE;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction.OUTDEGREE;

public class JaccardIndex implements UnaryGraphToGraphOperator{

  /**
   * Default Key for Result
   **/
  public static final String DEFAULT_JACCARD_EDGE_PROPERTY = "value";
  /**
   * Group size for the quadratic expansion of neighbor pairs
   **/
  private static final int DEFAULT_GROUP_SIZE = 64;
  /**
   * Default Edge Label for Results
   **/
  private static final String DEFAULT_JACCARD_EDGE_LABEL = "jaccardSimilarity";
  private String edgeLabel = DEFAULT_JACCARD_EDGE_LABEL;
  private Direction direction = OUTDEGREE;

  public void setDirection(Direction direction) {
    this.direction = direction;
  }

  public void setEdgeLabel(String edgeLabel) {
    this.edgeLabel = edgeLabel;
  }

  private LogicalGraph executeInternal(LogicalGraph inputGraph) throws Exception {

    System.out.println("INPUT");
    inputGraph.getEdges().print();

    // VertexDegrees
    UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> degreeOperator =
      getDegreeOperator(direction);
    DataSet<WithCount<GradoopId>> degrees = degreeOperator.execute(inputGraph);

    // TODO als Function
    DataSet<Tuple3<GradoopId, GradoopId, Long>> edgesWithDegree =
      inputGraph.getEdges().join(degrees).where(new ConfigurableEdgeKeySelector(direction))
        .equalTo(new KeySelector<WithCount<GradoopId>, GradoopId>() {
          @Override
          public GradoopId getKey(WithCount<GradoopId> value) {
            return value.getObject();
          }
        }).with(new JoinFunction<Edge, WithCount<GradoopId>, Tuple3<GradoopId, GradoopId, Long>>() {
        @Override
        public Tuple3<GradoopId, GradoopId, Long> join(Edge edge,
          WithCount<GradoopId> vertexDegree) {
          return new Tuple3<>(edge.getSourceId(), edge.getTargetId(), vertexDegree.getCount());
        }
      });

    System.out.println("------------ EDGES WITH DEGREES --------------");
    edgesWithDegree.print();


    // group span, source, target, degree(t/s), je nach einstellung
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groupSpans =
      edgesWithDegree.groupBy(0).sortGroup(1, Order.ASCENDING)
        .reduceGroup(new GenerateGroupSpans(DEFAULT_GROUP_SIZE)).setParallelism(PARALLELISM_DEFAULT)
        .name("Generate group spans");

    // group, s, t, d(t)
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groups =
      groupSpans.rebalance().setParallelism(PARALLELISM_DEFAULT).name("Rebalance")
        .flatMap(new GenerateGroups()).setParallelism(PARALLELISM_DEFAULT).name("Generate groups");
    System.out.println("------------ GROUPS --------------");
    groups.print();

    // t, u, d(t), d(u)
    System.out.println("------------ NXT GROUPBY --------------");

    groups.groupBy(0, direction.equals(INDEGREE) ? 1 : 2).reduceGroup(
      new GroupReduceFunction<Tuple4<IntValue, GradoopId, GradoopId, IntValue>, Tuple2<IntValue,
        String>>() {
        @Override
        public void reduce(Iterable<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> values,
          Collector<Tuple2<IntValue, String>> out) {
          int groupSize = 0;
          StringBuilder builder = new StringBuilder();
          for (Tuple4<IntValue, GradoopId, GradoopId, IntValue> value : values) {
            groupSize++;
            builder.append("(" + value.f1 + ")-[" + value.f3 + "]->" + "(" + value.f2 + ") ");
          }
          out.collect(new Tuple2<>(new IntValue(groupSize), builder.toString()));
        }
      }).print();


    DataSet<Tuple3<GradoopId, GradoopId, IntValue>> twoPaths = groups
      .groupBy(0, direction.equals(INDEGREE) ? 1 : 2)  // TODO dieses groupBy ergibt die richtigen
      // paare TODO: dafür komme ich in die clean code hölle
      .sortGroup(1, Order.ASCENDING)
      .reduceGroup(new GenerateGroupPairs(DEFAULT_GROUP_SIZE, direction))
      .name("Generate group pairs");
    System.out.println("------------ TWO PATHS --------------");
    twoPaths.print();

    // t, u, intersection, union
    DataSet<Edge> scoreEdges =
      twoPaths.groupBy(0, 1).reduceGroup(new ComputeScores(edgeLabel)).name("Compute scores");

    System.out.println("------------ SCORES --------------");
    scoreEdges.print();

    DataSet<Edge> union = scoreEdges.union(inputGraph.getEdges());

    return inputGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(inputGraph.getVertices(), union);

  }

  private UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> getDegreeOperator(
    Direction direction) {

    if (direction.equals(INDEGREE)) {
      return new IncomingVertexDegrees();
    }

    return new OutgoingVertexDegrees();
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    try {
      return  executeInternal(graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public enum Direction {INDEGREE, OUTDEGREE}

  public enum Denominator {UNION, MAX}

  @Override
  public String getName() {
    return JaccardIndex.class.getName();
  }
}
package org.gradoop.flink.model.impl.operators.join.joinwithfusion;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MergeGraphHeads;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MergeToBeFusedVertices;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.DemultiplexEdge;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.DemultiplexVertex;

/**
 * Created by vasistas on 15/02/17.
 */
public class Testing {

  /**
   * Filters a collection form a graph dataset performing either an intersection or a difference
   *
   * @param collection Collection to be filtered
   * @param g          Graph where to verify the containment operation
   * @param inGraph    If the value is true, then perform an intersection, otherwise a difference
   * @param <P>        e.g. either vertices or edges
   * @return The filtered collection
   */
  public static <P extends GraphElement> DataSet<P> fusionUtilsAreElementsInGraph(DataSet<P>
    collection,
    LogicalGraph g, boolean inGraph) {
    return collection
      .filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(g.getGraphHead().map(new Id<>()), GraphContainmentFilterBroadcast.GRAPH_ID);
  }

  public static void main(String args[]) {
    GraphCollection patterns = null;
    LogicalGraph left = null;
    LogicalGraph right = null;
    Function<Tuple2<String,String>,String> concatenateGraphHeads = null;
    Function<Tuple2<Properties,Properties>,Properties> concatenateProperties = null;
    JoinType vertexJoinType = null;

    DataSet<Vertex> vertexUnion = left.getVertices()
      .union(right.getVertices());

    // getting all the possible graphs
    DataSet<GraphHead> graphHeads = patterns.getGraphHeads();
      //.map(new Id<>());

    // The resulting graph id
    GradoopId fusedGraphId = GradoopId.get();
    // The head belonging to the final graph
    DataSet<GraphHead> fusedGraphHead = left.getGraphHead()
      .first(1)
      .join(right.getGraphHead().first(1))
      .where((GraphHead x)->0).equalTo((GraphHead y)->0)
      .with(new MergeGraphHeads(fusedGraphId,concatenateGraphHeads,concatenateProperties));

    // ---------------
    // - Step 1
    // ---------------
    // associate to each vertex its graphId <graphid,vertex>
    DataSet<Tuple2<GradoopId, Vertex>> demultiplexedVertices = patterns.getVertices()
      .flatMap(new DemultiplexVertex())
      .join(graphHeads)
      .where(new Value0Of2<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    // associate to each edge its graphId <graphid,edge>
    DataSet<Tuple2<GradoopId, Edge>> demultiplexedEdges = patterns.getEdges()
      .flatMap(new DemultiplexEdge())
      .join(graphHeads)
      .where(new Value0Of2<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    // ---------------
    // - Step 2
    // ---------------
    // filter the demultiplexed vertices such that they appear in the union graph
    DataSet<Tuple3<Vertex, Boolean, GradoopId>> toBefusedVertices = vertexUnion
      .join(demultiplexedVertices)
      .where(new Id<>()).equalTo(new EPGMElementIdInSecond<>())
      .with(new FlatJoinFunction<Vertex, Tuple2<GradoopId, Vertex>, Tuple3<Vertex, Boolean, GradoopId>>() {
        @Override
        public void join(Vertex first, Tuple2<GradoopId, Vertex> second,
          Collector<Tuple3<Vertex, Boolean, GradoopId>> out) throws Exception {
          out.collect(new Tuple3<>());
        }
      });

      //demultiplexedVertices
      //.join(vertexUnion)
      //.where(new EPGMElementIdInSecond<>()).equalTo(new Id<>())
      //.with(new LeftSide<>());

    // the patterns that are actually used are the ones that have a GradoopId in the
    // toBefusedVertices set. Merge the filtered vertices as pointed out by the patterns
    //DataSet<Tuple2<GradoopId, Vertex>> fusedVertices = toBefusedVertices
      //.coGroup(graphHeads);
      /*.where(new Value0Of2<>()).equalTo(new Id<>())
      .with(new MergeToBeFusedVertices(fusedGraphId))*/;

      /*
    DataSet<Vertex> toBeReturned = fusedVertices.map(new Value1Of2<>());
    switch (vertexJoinType) {
    case INNER:
      break;
    case LEFT_OUTER:
      toBeReturned.union()
      break;
    case RIGHT_OUTER:
      break;
    case FULL_OUTER:
      break;
    }*/

  }
  
}

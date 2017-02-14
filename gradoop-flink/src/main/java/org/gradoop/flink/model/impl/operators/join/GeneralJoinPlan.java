/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.join;

import com.sun.istack.Nullable;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.join.blocks.CoJoinGraphHeads;
import org.gradoop.flink.model.impl.operators.join.blocks.FunctionToKeySelector;
import org.gradoop.flink.model.impl.operators.join.blocks.KeySelectorFromRightProjection;
import org.gradoop.flink.model.impl.operators.join.blocks
  .KeySelectorFromTupleProjetionWithGradoopId;
import org.gradoop.flink.model.impl.operators.join.blocks.KeySelectorFromTupleProjetionWithTargetId;
import org.gradoop.flink.model.impl.operators.join.blocks.VertexJoinCondition;
import org.gradoop.flink.model.impl.operators.join.blocks.VertexPJoinCondition;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.functions.OplusHeads;
import org.gradoop.flink.model.impl.operators.join.functions.OplusVertex;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.operators.PreFilter;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;
import org.gradoop.flink.model.impl.operators.join.tuples.ResultingJoinVertex;

import java.util.Objects;

/**
 * Created by Giacomo Bergami on 30/01/17.
 *
 * @param <PV> value over which, eventually, perform the multi-key join
 *             (activated by the PreFilter parameters)
 */
public class GeneralJoinPlan<PV> implements BinaryGraphToGraphOperator {

  private final PreFilter<Vertex, PV> leftPrefilter;
  private final PreFilter<Vertex, PV> rightPrefilter;
  private final JoinType vertexJoinType;
  private final CoJoinGraphHeads cojoingraphheads;
  private final VertexPJoinCondition<PV> verexPJoinCond;
  private KeySelectorFromRightProjection projector;
  private final RichFlatJoinFunction<Vertex, Vertex, ResultingJoinVertex> vertexJoinCond;
  private MapFunction<Tuple2<Vertex,OptSerializableGradoopId>, Vertex> mapper = null;
  private final FunctionToKeySelector leftHash;
  private final FunctionToKeySelector rightHash;
  private final Function<Tuple2<Vertex,Vertex>,Boolean> thetaVertex;
  private final Function<Tuple2<GraphHead,GraphHead>,Boolean> thetaGraph;
  private final OplusVertex combineVertices;
  private final OplusHeads combineHeads;
  private DataSet<Tuple2<GradoopId, Vertex>> leftV, rightV;
  private final GeneralEdgeSemantics edgeSemanticsImplementation;

  private DataSet<ResultingJoinVertex> lrVjoin;

  /**
   * This class defines the general interface for all the possible graph join operations.
   * <p>
   * The two logical graphs are joined with a non-empty result iff. the properties of the
   * two graphs match as defined in {@code thetaGraph}
   * <p>
   * Please note that:
   * * If both {@code leftPreFilter} and {@code rightPreFilter} are choosed, then a join is
   * done over the {@code <PV>} parameter value
   * * If either {@code leftPreFilter} or {@code rightPreFilter} are not null, then the non
   * null value is re-mapped into a DataSet of vertices and used in the join for the following
   * case:
   * * If both {@code leftPreFilter} and {@code rightPreFilter} are null, then the graph join
   * is performed using the hashing functions {@code leftHash} and {@code rightHash}
   * <p>
   * In all the previously depicted scenarios, the vertices undergo a filtering function,
   * {@code thetaVertex}.
   *
   * @param vertexJoinType              A Inner/Left/Right/Full graph join depend on the
   *                                    Inner/Left/Right/Full join performed over the vertices
   * @param edgeSemanticsImplementation This class casts all the information about the way to
   *                                    combine the edges
   * @param leftPreFilter               Non-Serializable function for mapping a vertex DataSet
   *                                    from the left graph into a dataset where vertices
   *                                    are extended with their joining key (ideal when
   *                                    there could be multiple join keys for the same vertex)
   * @param rightPreFilter              Non-Serializable function for mapping a vertex DataSet
   *                                    from the right graph into a dataset where vertices
   *                                    are extended with their joining key (ideal when
   *                                    there could be multiple join keys for the same vertex)
   * @param leftHash                    Hashing function used when both PreFilters are not used
   *                                    for the vertices of the left graph. If it is {@code
   *                                    null}, then the {@code 0L} constant function is
   *                                    used instead
   * @param rightHash                   Hashing function used when both PreFilters are not used
   *                                    for the vertices of the right graph. If it is {@code
   *                                    null}, then the {@code 0L} constant function is
   *                                    used instead
   * @param thetaVertex                 Predicate test function to be used over the vertices
   *                                    that undergo a join. It'll be extended with the
   *                                    {@code Properties} validity test. Such test consists
   *                                    into verifying if the graph vertex has property keys
   *                                    with the same value.
   * @param thetaGraph                  Predicate test used to check if the graphs must undergo
   *                                    the joining algorithm.
   * @param vertexLabelConcatenation    Given the vertex labels belonging to the vertices that
   *                                    have to be fused, return the new label obtained from the
   *                                    two original ones
   * @param graphLabelConcatenation     Given the graph label belonging to the to-be-joined
   *                                    graphs, returns a graph with such label if the graph
   *                                    conditions are met
   */
  public GeneralJoinPlan(

    JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,

    @Nullable PreFilter<Vertex, PV> leftPreFilter, @Nullable PreFilter<Vertex, PV> rightPreFilter,

    @Nullable Function<Vertex, Long> leftHash, @Nullable Function<Vertex, Long> rightHash,

    @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,

    @Nullable Function<Tuple2<String,String>,String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String,String>,String> graphLabelConcatenation) {


    this.thetaGraph = JoinUtils.extendBasic(thetaGraph);
    this.thetaVertex = JoinUtils.extendBasic(thetaVertex);

    this.combineHeads = new OplusHeads(JoinUtils.generateConcatenator(graphLabelConcatenation));
    this.combineVertices = new OplusVertex(JoinUtils.generateConcatenator(vertexLabelConcatenation));

    this.leftPrefilter = leftPreFilter;
    this.rightPrefilter = rightPreFilter;
    this.vertexJoinType = vertexJoinType;
    this.edgeSemanticsImplementation = edgeSemanticsImplementation;

    //this.edgeJoinType = edgeJoinType;
    this.projector = null;
    this.leftHash = new FunctionToKeySelector(leftHash);
    this.rightHash = new FunctionToKeySelector(rightHash);
    this.vertexJoinCond = new VertexJoinCondition(this.thetaVertex,combineVertices);
    this.verexPJoinCond = new VertexPJoinCondition<>(this.thetaVertex,combineVertices);
    this.cojoingraphheads = new CoJoinGraphHeads(this.thetaGraph,combineHeads);
  }


  @Override
  public String getName() {
    return GeneralJoinPlan.class.getName();
  }


  @Override
  public LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph) {

    clear();
    Objects.requireNonNull(firstGraph, "first graph operator is null");
    Objects.requireNonNull(secondGraph, "second graph operator is null");

    final GradoopId gid = GradoopId.get();
    DataSet<GraphHead> gh =
      firstGraph.getGraphHead()
        .coGroup(secondGraph.getGraphHead())
        .where((GraphHead x) -> 0)
        .equalTo((GraphHead y) -> 0)
        .with(cojoingraphheads.setGraphId(gid));


    joinVertices(firstGraph.getVertices(), secondGraph.getVertices());

    DataSet<Vertex> vertices = lrVjoin.crossWithTiny(gh.first(1))
      .with(new CrossFunction<ResultingJoinVertex, GraphHead, Vertex>() {
        @Override
        public Vertex cross(ResultingJoinVertex val1, GraphHead val2) throws Exception {
          Vertex toret = val1.f2;
          toret.addGraphId(val2.getId());
          return toret;
        }
      }).distinct((Vertex v)->v.getId());


    DataSet<CombiningEdgeTuples> leftE = joinEdgePerGraph(firstGraph.getEdges(),leftV,0);
    DataSet<CombiningEdgeTuples> rightE = joinEdgePerGraph(secondGraph.getEdges(),rightV,1);

    DataSet<Edge> edges = joinEdges(leftE, rightE).crossWithTiny(gh.first(1))
      .with(new CrossFunction<Edge, GraphHead, Edge>() {
        @Override
        public Edge cross(Edge val1, GraphHead val2) throws Exception {
          val1.addGraphId(val2.getId());
          return val1;
        }
      });

    return LogicalGraph.fromDataSets(gh, vertices, edges, firstGraph.getConfig());
  }

  private void clear() {
    lrVjoin = null;
  }

  private DataSet<CombiningEdgeTuples> joinEdgePerGraph(DataSet<Edge> edges,
    DataSet<Tuple2<GradoopId, Vertex>> leftOrRight, int which) {
    return leftOrRight.join(edges)
      .where(new KeySelectorFromTupleProjetionWithGradoopId())
      .equalTo((Edge e) -> e.getSourceId())
      .with(new JoinFunction<Tuple2<GradoopId, Vertex>, Edge, Tuple2<Vertex, Edge>>() {
        @Override
        public Tuple2<Vertex, Edge> join(Tuple2<GradoopId, Vertex> first, Edge second) throws
          Exception {
          return new Tuple2<>(first.f1, second);
        }
      }).returns(TypeInfoParser.parse(
        Tuple2.class.getCanonicalName() + "<" + Vertex.class.getCanonicalName() + "," +
          Edge.class.getCanonicalName() + ">")).join(leftOrRight)
      .where(new KeySelectorFromTupleProjetionWithTargetId())
      .equalTo(new KeySelectorFromTupleProjetionWithGradoopId()).with(
        new JoinFunction<Tuple2<Vertex, Edge>, Tuple2<GradoopId, Vertex>, CombiningEdgeTuples>() {
          @Override
          public CombiningEdgeTuples join(Tuple2<Vertex, Edge> first,
            Tuple2<GradoopId, Vertex> second) throws Exception {
            return new CombiningEdgeTuples(first.f0, first.f1, second.f1);
          }
        }).returns(CombiningEdgeTuples.class);

  }

  private void joinVertices(DataSet<Vertex> vertices, DataSet<Vertex> vertices1) {
    DataSet<Vertex> left = vertices, right = vertices1;
    DataSet<Tuple2<Vertex,OptSerializableGradoopId>> leftP = null, rightP = null;
    boolean leftFilter = false, rightFilter = false;

    /*
     * Sometimes I could pre-filter the vertices and demultiplex them…
     */
    if (leftPrefilter != null) {
      leftP = leftPrefilter.apply(left);
      leftFilter = true;
    }
    if (rightPrefilter != null) {
      rightP = rightPrefilter.apply(right);
      rightFilter = true;
    }

    if (leftFilter && rightFilter) {
      /*
       * When we have a demultiplex, then we join by the multiplex condition
       */
      if (projector == null) {
        projector = new KeySelectorFromRightProjection();
      }
      lrVjoin =
        JoinUtils.joinByType(leftP, rightP, vertexJoinType)
          .where(projector)
          .equalTo(projector)
          .with(verexPJoinCond)
          .returns(ResultingJoinVertex.class);

    } else {
      /*
       * Otherwise, join the vertices with the usual join condition
       */
      if ((mapper == null) && (leftFilter || rightFilter)) {
        mapper = (Tuple2<Vertex,OptSerializableGradoopId> t) -> t.f0;
      }
      if (leftFilter) {
        left = leftP.map(mapper).distinct();
      }
      if (rightFilter) {
        right = rightP.map(mapper).distinct();
      }
      lrVjoin = JoinUtils.joinByType(left, right, vertexJoinType)
        .where(leftHash)
        .equalTo(rightHash)
        .with(vertexJoinCond);
    }
    leftV = lrVjoin.filter((ResultingJoinVertex x) -> x.f0.isThereElement)
      .map((ResultingJoinVertex x) -> new Tuple2<GradoopId, Vertex>(x.f0.get(), x.f2))
      .returns(TypeInfoParser.parse(Tuple2.class.getCanonicalName()+"<"+GradoopId.class
        .getCanonicalName()+"," +
        ""+Vertex.class
          .getCanonicalName()+">"));
    rightV = lrVjoin.filter((ResultingJoinVertex x) -> x.f1.isThereElement)
      .map((ResultingJoinVertex x) -> new Tuple2<GradoopId, Vertex>(x.f1.get(), x.f2))
      .returns(TypeInfoParser.parse(Tuple2.class.getCanonicalName()+"<"+GradoopId.class
        .getCanonicalName()+"," +
        ""+Vertex.class
        .getCanonicalName()+">"));
  }

  private DataSet<Edge> joinEdges(DataSet<CombiningEdgeTuples> left,
    DataSet<CombiningEdgeTuples> right) {

    return JoinUtils.joinByType(left, right, edgeSemanticsImplementation.edgeJoinType)
      .where((CombiningEdgeTuples t) -> t.f0.getId().hashCode() * 13 + t.f2.getId().hashCode())
      .equalTo((CombiningEdgeTuples t) -> t.f0.getId().hashCode() * 13 + t.f2.getId().hashCode())
      .with(edgeSemanticsImplementation.joiner)
      .returns(Edge.class);

  }


}

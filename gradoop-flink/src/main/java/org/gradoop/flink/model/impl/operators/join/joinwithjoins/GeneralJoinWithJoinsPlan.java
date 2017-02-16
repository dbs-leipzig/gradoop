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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins;

import com.sun.istack.Nullable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.CoJoinGraphHeads;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .JoinFunctionFlatWithGradoopIds;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .JoinFunctionVertexJoinCondition;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.KeySelectorFromFunction;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .KeySelectorFromRightProjection;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .KeySelectorTripleHashfunction;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .CrossFunctionAddUndovetailingToGraph;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.OplusHeads;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.OplusVertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.PreFilter;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.JoinWithJoinsUtils;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .CrossFunctionAddEpgmElementToGraphThroughGraphHead;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .JoinFunctionAssociateVertexWithEdge;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .KeySelectorFromTupleProjetionWithTargetId;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .JoinFunctionCreateTriple;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .FilterFunctionIsThereElement;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .MapFunctionProjectUndovetailingToGraphOperand;
/**
 *
 * General abstract class with everything required to implement every possible graph join definition
 * formally.
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class GeneralJoinWithJoinsPlan implements BinaryGraphToGraphOperator {

  /**
   * Returning the GradoopId belonging to one of the operands' vertices
   */
  private static final Value0Of2<GradoopId, Vertex> LEFT_FUNCTION_PROJECTION = new Value0Of2<>();

  /**
   * Hashing the triples (vertices from the to-be-returned graph and the edge from one of the two
   * operands) by ignoring the aforementioned edge and hashing by vertices' id
   */
  private static final KeySelectorTripleHashfunction TRIPLES_HASH = new
    KeySelectorTripleHashfunction();

  /**
   * Hashing function used for joining together elements matched by the same edge
   */
  private static final KeySelectorFromRightProjection PROJECTOR = new
    KeySelectorFromRightProjection();

  /** A Inner/Left/Right/Full graph join depend on the
   * Inner/Left/Right/Full join performed over the vertices
   */
  private final JoinType vertexJoinType;

  /** This class casts all the information about the way to combine the edges
   */
  private final GeneralEdgeSemantics edgeSemanticsImplementation;

  /** Non-Serializable function for mapping a vertex DataSet
   * from the left graph into a dataset where vertices
   * are extended with their joining key (ideal when
   * there could be multiple join keys for the same vertex)
   */
  private final PreFilter<Vertex> leftPrefilter;

  /** Non-Serializable function for mapping a vertex DataSet
   * from the right graph into a dataset where vertices
   * are extended with their joining key (ideal when
   * there could be multiple join keys for the same vertex)
   */
  private final PreFilter<Vertex>  rightPrefilter;

  /**
   * The user-defined hash function for left vertices is wrapped, so that if no function is given,
   * then the vertices are automatically mapped to a zero-constant.
   */
  private final KeySelectorFromFunction leftHash;

  /**
   * The user-defined hash function for right vertices is wrapped, so that if no function is given,
   * then the vertices are automatically mapped to a zero-constant.
   */
  private final KeySelectorFromFunction rightHash;

  /**
   * Defines how to combine the vertices for the final result by combining the theta function
   * for the vertices and the way to combine the vertices to create one finally fused vertex.
   */
  private final JoinFunctionVertexJoinCondition vertexJoinCond;

  /**
   * Defines how to combine the vertices for the final result by combining the theta function
   * for the vertices and the way to combine the vertices to create one finally fused vertex.
   */
  private final JoinFunctionFlatWithGradoopIds verexPJoinCond;

  /**
   * Function defining how to combine the graph heads together
   */
  private final CoJoinGraphHeads cojoingraphheads;

  /**
   * Provides the operands' vertices from the previous match with the relation
   */
  private Value0Of3<Vertex,Boolean,GradoopId> mapper;

  /**
   * Part of the inernal state of the intermediate computation of the vertices (1 out of 3)
   * This lrVjoin stores the partial informaitons required to finally create the vertices.
   * This inofmration is used to reconstruct the associations between the old operands' vertices
   * and the newly created ones
   */
  private DataSet<UndovetailingOPlusVertex> lrVjoin;

  /** Part of the inernal state of the intermediate computation of the vertices (2 out of 3)
   *  This leftV stores the associations between the old graph vertex id and the newly created
   *  vertex for the left graph operand
   */
  private DataSet<Tuple2<GradoopId, Vertex>> leftV;

  /** Part of the inernal state of the intermediate computation of the vertices (3 out of 3)
   *  This rightV stores the associations between the old graph vertex id and the newly created
   *  vertex for the right graph operand
   */
  private DataSet<Tuple2<GradoopId, Vertex>>  rightV;



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
  public GeneralJoinWithJoinsPlan(

    JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,

    @Nullable PreFilter<Vertex> leftPreFilter, @Nullable PreFilter<Vertex> rightPreFilter,

    @Nullable Function<Vertex, Long> leftHash, @Nullable Function<Vertex, Long> rightHash,

    @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,

    @Nullable Function<Tuple2<String, String>, String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String, String>, String> graphLabelConcatenation) {

    /*
      Some functions are only defined logically, but used practially with wrappers. So intermediate
      steps are just pre-computed
     */
    Function<Tuple2<GraphHead, GraphHead>, Boolean> thetaGraph1 =
      JoinWithJoinsUtils.extendBasic(thetaGraph);
    Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertex1 =
      JoinWithJoinsUtils.extendBasic(thetaVertex);
    OplusHeads combineHeads =
      new OplusHeads(JoinWithJoinsUtils.generateConcatenator(graphLabelConcatenation));
    OplusVertex combineVertices =
      new OplusVertex(JoinWithJoinsUtils.generateConcatenator(vertexLabelConcatenation));

    this.vertexJoinType = vertexJoinType;
    this.edgeSemanticsImplementation = edgeSemanticsImplementation;
    this.leftPrefilter = leftPreFilter;
    this.rightPrefilter = rightPreFilter;
    this.leftHash = new KeySelectorFromFunction(leftHash);
    this.rightHash = new KeySelectorFromFunction(rightHash);

    this.vertexJoinCond = new JoinFunctionVertexJoinCondition(thetaVertex1, combineVertices);
    this.verexPJoinCond = new JoinFunctionFlatWithGradoopIds(thetaVertex1, combineVertices);
    this.cojoingraphheads = new CoJoinGraphHeads(thetaGraph1, combineHeads);
    mapper = new Value0Of3<>();
  }

  @Override
  public String getName() {
    return GeneralJoinWithJoinsPlan.class.getName();
  }

  @Override
  public LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph) {
    clear();

    // Defining the graph id for the graph that has to be returned
    final GradoopId gid = GradoopId.get();

    // Generating the graph head if the two graph operands match
    DataSet<GraphHead> gh = firstGraph.getGraphHead()
      .coGroup(secondGraph.getGraphHead())
      .where((GraphHead x) -> 0).equalTo((GraphHead y) -> 0)
      .with(cojoingraphheads.setGraphId(gid));

    // Join the vertices together. This function updates the following variables:
    // * <code>lrVJoin<code>: "raw" vertices to appear in the resulting graph
    // * <code>leftV</code> and <code>rightV</code>: Association between the id matched within
    //                        the operands and the vertex to appear in the resulting graph
    joinVertices(firstGraph.getVertices(), secondGraph.getVertices());

    // Return them only if the graph has been actually created
    DataSet<Vertex> vertices = lrVjoin.crossWithTiny(gh.first(1))
      .with(new CrossFunctionAddUndovetailingToGraph())
      .distinct(new Id<>());

    /*
     * For each graph operand, generate a triple. In this case a triple is formed
     * by <src,e,dst>, where src and dst are the vertices appearing in the final graph,
     * and e is the edge in one in the two operands that will connect two vertices in
     * the final source.
     */
    DataSet<Triple> leftE = joinEdgePerGraphViaTriples(firstGraph.getEdges(), leftV);
    DataSet<Triple> rightE = joinEdgePerGraphViaTriples(secondGraph.getEdges(), rightV);

    /*
     * <code>edgeSemanticsImplementation</code>: the edges are joined according to the edge-join
     * function outlined by the semantic
     */
    DataSet<Edge> edges = JoinWithJoinsUtils.joinByType(leftE, rightE, edgeSemanticsImplementation
      .getEdgeJoinType())
      /*
       * The co-grouping condition is that the two edges appearing in two distinct graph
       * operands must link the same two vertices in the final graph. An hashing function
       * between source and destination graph id is then used (<code>tripleHash</code>)
       */
      .where(TRIPLES_HASH).equalTo(TRIPLES_HASH)
      /*
       * How the final edge should be obtained from the pair of triples is defined by the edge
       * semantics
       */
      .with(edgeSemanticsImplementation.getJoiner())
      /*
       * Return such elements if and only if the graph has actually to appear
       */
      .crossWithTiny(gh.first(1))
      /*
       * Add the edge in the final graph by using the graphId value stored within the graph head
       */
      .with(new CrossFunctionAddEpgmElementToGraphThroughGraphHead<>());

    return LogicalGraph.fromDataSets(gh, vertices, edges, firstGraph.getConfig());
  }

  /**
   * Function clearing the previous state
   */
  private void clear() {
    lrVjoin = null;
    leftV = null;
    rightV = null;
    mapper = null;
  }

  /**
   * Joins the vertices using the operands' edges
   *
   * @param edges                 Edges appearing in one of the two operands
   * @param verticesToBeReturned  Association between the old vertex id from the graph operand and
   *                              the newly-created vertices
   * @return                      the final triples combining the vertices from the to-be-returned
   *                              graph and the edge from the original graph operand
   */
  private DataSet<Triple> joinEdgePerGraphViaTriples(DataSet<Edge> edges,
    DataSet<Tuple2<GradoopId, Vertex>> verticesToBeReturned) {
    return verticesToBeReturned
      .join(edges)
      .where(LEFT_FUNCTION_PROJECTION).equalTo(new SourceId<>())
      .with(new JoinFunctionAssociateVertexWithEdge())
      .join(verticesToBeReturned)
      .where(new KeySelectorFromTupleProjetionWithTargetId()).equalTo(LEFT_FUNCTION_PROJECTION)
      .with(new JoinFunctionCreateTriple());
  }

  /**
   * Implements the join operation for the vertices. This function updates the operator's state,
   * by associating values to <code>lrVjoin</code>, <code>leftV</code> and <code>rightV</code>
   * @param leftGraphVertices   vertices coming for the left operand
   * @param rightGraphVertices  edges coming from the right operand
   */
  private void joinVertices(DataSet<Vertex> leftGraphVertices, DataSet<Vertex> rightGraphVertices) {
    DataSet<Vertex> left = leftGraphVertices;
    DataSet<Vertex> right = rightGraphVertices;
    DataSet<Tuple3<Vertex, Boolean, GradoopId>> leftP = null;
    DataSet<Tuple3<Vertex, Boolean, GradoopId>> rightP = null;
    boolean leftFilter = false;
    boolean rightFilter = false;

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
      lrVjoin =
        JoinWithJoinsUtils.joinByType(leftP, rightP, vertexJoinType)
          .where(PROJECTOR).equalTo(PROJECTOR)
          .with(verexPJoinCond);

    } else {
      /*
       * Otherwise, join the vertices with the usual join condition
       */
      if (leftFilter) {
        left = leftP.map(mapper).distinct();
      }
      if (rightFilter) {
        right = rightP.map(mapper).distinct();
      }
      lrVjoin = JoinWithJoinsUtils.joinByType(left, right, vertexJoinType)
        .where(leftHash)
        .equalTo(rightHash)
        .with(vertexJoinCond);
    }
    // From the resulting filtered vertices, get only those that were involved in the
    // match operation. Assoicate each GradoopId from the graph operand with the newly-created
    // vertex that will appear in the final graph.
    leftV = lrVjoin
      .filter(new FilterFunctionIsThereElement(true))
      .map(new MapFunctionProjectUndovetailingToGraphOperand(true));
    rightV = lrVjoin
      .filter(new FilterFunctionIsThereElement(false))
      .map(new MapFunctionProjectUndovetailingToGraphOperand(false));
  }

}

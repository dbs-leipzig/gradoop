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

package org.gradoop.flink.model.impl.operators.matching.transactional;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Project4To0And1;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingEdges;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.AddMatchesToProperties;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.BuildIdWithCandidatesAndGraphs;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.BuildTripleWithCandidatesAndGraphs;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.ConstructGraphs;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.ExpandFirstField;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.FindEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.HasEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.InitGraphHeadWithLineage;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.MergeSecondField;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.Project4To0And2AndSwitch;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.Project4To0And3AndSwitch;

import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;
import org.gradoop.flink.model.impl.operators.subgraph.functions.AddGraphsToElements;

/**
 * Operator to match a given pattern on a graph collection
 */
public class TransactionalPatternMatching implements UnaryCollectionToCollectionOperator {
  /**
   * Query Pattern
   */
  private String query;
  /**
   * Actual used algorithm
   */
  private PatternMatchingAlgorithm algorithm;

  /**
   * Flag that determines what the result consists of:
   * false: source graphs with an new property that is true iff the pattern
   *        was found in this graph
   * true:  one graph per found embedding
   */
  private boolean findEmbeddings;

  /**
   * Constructor
   *
   * @param algorithm pattern matching algorithm
   * @param query     given query-pattern
   * @param findEmbeddings  flag that determines the type of the return
   *                        false:  source graphs with an new property that is
   *                                true iff the pattern was found in this graph
   *                        true:   one graph per found embedding
   */
  public TransactionalPatternMatching(
    String query,
    PatternMatchingAlgorithm algorithm,
    boolean findEmbeddings) {
    this.query = query;
    this.algorithm = algorithm;
    this.findEmbeddings = findEmbeddings;
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    //--------------------------------------------------------------------------
    // generate graph-id set witch will be used for broadcasting
    //--------------------------------------------------------------------------
    DataSet<GradoopId> graphIds = collection.getGraphHeads().map(new Id<>());

    //--------------------------------------------------------------------------
    // generate mapping from graph-id to vertex candidates
    //--------------------------------------------------------------------------
    DataSet<Tuple2<GradoopId, IdWithCandidates<GradoopId>>>
      vertexCandidatesWithGraphs = collection.getVertices()
        .filter(new MatchingVertices<>(query))
        .map(new BuildIdWithCandidatesAndGraphs<>(query))
        .flatMap(new ExpandFirstField<>())
        .join(graphIds)
        .where(0).equalTo("*")
        .with(new LeftSide<>());


    //--------------------------------------------------------------------------
    // generate mapping from graph-id to edge candidates
    //--------------------------------------------------------------------------
    DataSet<Tuple2<GradoopId, TripleWithCandidates<GradoopId>>>
      edgeCandidatesWithGraphs =
      collection.getEdges()
        .filter(new MatchingEdges<>(query))
        .map(new BuildTripleWithCandidatesAndGraphs<>(query))
        .flatMap(new ExpandFirstField<>())
        .join(graphIds)
        .where(0).equalTo("*")
        .with(new LeftSide<>());

    //--------------------------------------------------------------------------
    // generate graphs with the candidates for their elements
    //--------------------------------------------------------------------------
    DataSet<GraphWithCandidates> graphs =
      vertexCandidatesWithGraphs
        .coGroup(edgeCandidatesWithGraphs)
        .where(0).equalTo(0)
        .with(new ConstructGraphs());

    if (findEmbeddings) {
      return findEmbeddings(collection, graphs);
    } else {
      return hasEmbeddings(collection, graphs);
    }
  }

  /**
   * Returns the input graph collection with a new property for each graph, that
   * states if it contains the embedding.
   * @param collection input graph collection
   * @param graphs graphs with candidates of their elements
   * @return input graph collection with new boolean property
   */
  private GraphCollection hasEmbeddings(GraphCollection collection,
    DataSet<GraphWithCandidates> graphs) {
    //--------------------------------------------------------------------------
    // run matching algorithm
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, Boolean>> matches = graphs.map(new HasEmbeddings(algorithm, query));

    //--------------------------------------------------------------------------
    // join matches to graph heads
    //--------------------------------------------------------------------------

    DataSet<GraphHead> newHeads =
      collection.getGraphHeads()
        .coGroup(matches)
        .where(new Id<>()).equalTo(0)
        .with(new AddMatchesToProperties());

    //--------------------------------------------------------------------------
    // return updated graph collection
    //--------------------------------------------------------------------------
    return GraphCollection.fromDataSets(
      newHeads, collection.getVertices(), collection.getEdges(), collection.getConfig());
  }


  /**
   * Finds all embeddings in the given graph and constructs a new graph
   * collection consisting of these embeddings.
   * @param collection input graph collection
   * @param graphs graphs with candidates of their elements
   * @return collection of found embeddings
   */
  private GraphCollection findEmbeddings(GraphCollection collection,
    DataSet<GraphWithCandidates> graphs) {

    //--------------------------------------------------------------------------
    // run the matching algorithm
    //--------------------------------------------------------------------------
    DataSet<Tuple4<GradoopId, GradoopId, GradoopIdSet, GradoopIdSet>> embeddings = graphs
        .flatMap(new FindEmbeddings(algorithm, query));

    //--------------------------------------------------------------------------
    // create new graph heads
    //--------------------------------------------------------------------------
    DataSet<GraphHead> newHeads = embeddings
      .map(new Project4To0And1<>())
      .map(new InitGraphHeadWithLineage(collection.getConfig().getGraphHeadFactory()));


    //--------------------------------------------------------------------------
    // update vertex graphs
    //--------------------------------------------------------------------------
    DataSet<Tuple2<GradoopId, GradoopIdSet>> verticesWithGraphs =
      embeddings
        .map(new Project4To0And2AndSwitch<>())
        .flatMap(new ExpandFirstField<>()).groupBy(0)
        .reduceGroup(new MergeSecondField<>());

    DataSet<Vertex> newVertices =
      verticesWithGraphs
        .join(collection.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new AddGraphsToElements<>());

    //--------------------------------------------------------------------------
    // update edge graphs
    //--------------------------------------------------------------------------
    DataSet<Tuple2<GradoopId, GradoopIdSet>> edgesWithGraphs =
      embeddings
        .map(new Project4To0And3AndSwitch<>())
        .flatMap(new ExpandFirstField<>()).groupBy(0)
        .reduceGroup(new MergeSecondField<>());

    DataSet<Edge> newEdges =
      edgesWithGraphs
        .join(collection.getEdges())
        .where(0).equalTo(new Id<>())
        .with(new AddGraphsToElements<>());


    //--------------------------------------------------------------------------
    // return the embeddings
    //--------------------------------------------------------------------------
    return GraphCollection.fromDataSets(newHeads, newVertices, newEdges, collection.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return TransactionalPatternMatching.class.getName();
  }
}

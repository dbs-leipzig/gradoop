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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.operators;

import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPEdgeCollection;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.EPVertexCollection;

/**
 * Describes all operators that can be applied on a single graph inside the
 * EPGM.
 */
public interface EPGraphOperators<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> {

  EPVertexCollection<VD> getVertices();

  EPEdgeCollection<ED> getEdges();

  EPEdgeCollection<ED> getOutgoingEdges(final Long vertexID);

  EPEdgeCollection<ED> getIncomingEdges(final Long vertexID);

  long getVertexCount() throws Exception;

  long getEdgeCount() throws Exception;

  /*
  unary operators take one graph as input and return a single graph or a
  graph collection
   */

  EPGraphCollection<VD, ED, GD> match(String graphPattern,
    Predicate<EPPatternGraph> predicateFunc);

  EPGraph<VD, ED, GD> project(UnaryFunction<VD, VD> vertexFunction,
    UnaryFunction<ED, ED> edgeFunction);

  <O extends Number> EPGraph<VD, ED, GD> aggregate(String propertyKey,
    UnaryFunction<EPGraph<VD, ED, GD>, O> aggregateFunc) throws Exception;

  /* Summarization */

  EPGraph<VD, ED, GD> summarize(String vertexGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarize(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexLabel() throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexLabel(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabel() throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception;

  EPGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabel(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  /*
  binary operators take two graphs as input and return a single graph
   */

  EPGraph<VD, ED, GD> combine(EPGraph<VD, ED, GD> otherGraph);

  EPGraph<VD, ED, GD> overlap(EPGraph<VD, ED, GD> otherGraph);

  EPGraph<VD, ED, GD> exclude(EPGraph<VD, ED, GD> otherGraph);

  /*
  auxiliary operators
   */
  EPGraph<VD, ED, GD> callForGraph(
    UnaryGraphToGraphOperator<VD, ED, GD> operator) throws Exception;

  EPGraph<VD, ED, GD> callForGraph(
    BinaryGraphToGraphOperator<VD, ED, GD> operator,
    EPGraph<VD, ED, GD> otherGraph);

  EPGraphCollection<VD, ED, GD> callForCollection(
    UnaryGraphToCollectionOperator<VD, ED, GD> operator);

  void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception;

}
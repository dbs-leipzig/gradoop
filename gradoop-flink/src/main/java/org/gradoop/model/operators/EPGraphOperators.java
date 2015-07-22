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

import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EPVertexData;
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
public interface EPGraphOperators {

  EPVertexCollection getVertices();

  EPEdgeCollection getEdges();

  EPEdgeCollection getOutgoingEdges(final Long vertexID);

  EPEdgeCollection getIncomingEdges(final Long vertexID);

  long getVertexCount() throws Exception;

  long getEdgeCount() throws Exception;

  /*
  unary operators take one graph as input and return a single graph or a
  graph collection
   */

  org.gradoop.model.impl.EPGraphCollection match(String graphPattern,
    Predicate<EPPatternGraph> predicateFunc);

  EPGraph project(UnaryFunction<EPVertexData, EPVertexData> vertexFunction,
    UnaryFunction<EPEdgeData, EPEdgeData> edgeFunction);

  <O extends Number> EPGraph aggregate(String propertyKey,
    UnaryFunction<EPGraph, O> aggregateFunc) throws Exception;

  /* Summarization */

  EPGraph summarize(String vertexGroupingKey) throws Exception;

  EPGraph summarize(String vertexGroupingKey, String edgeGroupingKey) throws
    Exception;

  EPGraph summarizeOnVertexLabel() throws Exception;

  EPGraph summarizeOnVertexLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  EPGraph summarizeOnVertexLabelAndEdgeProperty(String edgeGroupingKey) throws
    Exception;

  EPGraph summarizeOnVertexLabel(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  EPGraph summarizeOnVertexAndEdgeLabel() throws Exception;

  EPGraph summarizeOnVertexAndEdgeLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  EPGraph summarizeOnVertexAndEdgeLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception;

  EPGraph summarizeOnVertexAndEdgeLabel(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  /*
  binary operators take two graphs as input and return a single graph
   */

  EPGraph combine(EPGraph otherGraph);

  EPGraph overlap(EPGraph otherGraph);

  EPGraph exclude(EPGraph otherGraph);

  /*
  auxiliary operators
   */
  EPGraph callForGraph(UnaryGraphToGraphOperator operator) throws Exception;

  EPGraph callForGraph(BinaryGraphToGraphOperator operator, EPGraph otherGraph);

  EPGraphCollection callForCollection(UnaryGraphToCollectionOperator operator);

}
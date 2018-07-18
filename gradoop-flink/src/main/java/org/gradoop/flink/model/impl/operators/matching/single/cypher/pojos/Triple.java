/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos;


import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * This class represents a Triple.
 * A Triple represents an edge extended with information about the source and target vertex.
 */
public class Triple extends Tuple3<Vertex, Edge, Vertex> {

  /**
   * Default Constructor
   */
  public Triple() {
    super();
  }

  /**
   * Creates a new Triple
   * @param sourceVertex source vertex
   * @param edge edge
   * @param targetVertex target vertex
   */
  public Triple(Vertex sourceVertex, Edge edge, Vertex targetVertex) {
    super(sourceVertex, edge, targetVertex);
    requireValidTriple(sourceVertex, edge, targetVertex);
  }

  /**
   * Returns the source vertex.
   * @return source vertex
   */
  public Vertex getSourceVertex() {
    return f0;
  }

  /**
   * returns the edge
   * @return edge
   */
  public Edge getEdge() {
    return f1;
  }

  /**
   * Returns the target vertex
   * @return target vertex
   */
  public Vertex getTargetVertex() {
    return f2;
  }

  /**
   * Returns the source id
   * @return source id
   */
  public GradoopId getSourceId() {
    return f1.getSourceId();
  }

  /**
   * Returns the edge id
   * @return edge id
   */
  public GradoopId getEdgeId() {
    return f1.getId();
  }

  /**
   * Returns the target id
   * @return target id
   */
  public GradoopId getTargetId() {
    return f1.getTargetId();
  }

  /**
   * Ensures the validity of a triple. Requires that sourceVertex.id = edge.sourceId and
   * targetVertex.id = edge.targetId
   * @param sourceVertex source vertex
   * @param edge edge
   * @param targetVertex target vertex
   */
  private static void requireValidTriple(Vertex sourceVertex, Edge edge, Vertex targetVertex) {
    if (sourceVertex.getId() != edge.getSourceId()) {
      throw new IllegalArgumentException("Source IDs do not match");
    }

    if (targetVertex.getId() != edge.getTargetId()) {
      throw new IllegalArgumentException("Target IDs do not match");
    }
  }
}

/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.layout;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Represents a temporal graph or graph collection using three separate datasets:
 * <ol>
 *   <li>a dataset containing the temporal graph heads which are the metadata of logical graphs</li>
 *   <li>a dataset containing the temporal vertices</li>
 *   <li>a dataset containing the temporal edges</li>
 * </ol>
 */
public class TemporalGVELayout implements LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>,
  GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<TemporalGraphHead> temporalGraphHeads;
  /**
   * DataSet containing temporal vertices associated with that graph.
   */
  private final DataSet<TemporalVertex> temporalVertices;
  /**
   * DataSet containing temporal edges associated with that graph.
   */
  private final DataSet<TemporalEdge> temporalEdges;

  /**
   * Creates a new temporal layout holding the graph elements.
   *
   * @param temporalGraphHeads graph head dataset
   * @param temporalVertices vertex dataset
   * @param temporalEdges edge dataset
   */
  TemporalGVELayout(
    DataSet<TemporalGraphHead> temporalGraphHeads,
    DataSet<TemporalVertex> temporalVertices,
    DataSet<TemporalEdge> temporalEdges) {

    this.temporalGraphHeads = temporalGraphHeads;
    this.temporalVertices = temporalVertices;
    this.temporalEdges = temporalEdges;
  }

  @Override
  public DataSet<TemporalVertex> getVertices() {
    return this.temporalVertices;
  }

  @Override
  public DataSet<TemporalVertex> getVerticesByLabel(String label) {
    return this.temporalVertices.filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<TemporalEdge> getEdges() {
    return this.temporalEdges;
  }

  @Override
  public DataSet<TemporalEdge> getEdgesByLabel(String label) {
    return this.temporalEdges.filter(new ByLabel<>(label));
  }

  @Override
  public boolean isGVELayout() {
    return true;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public boolean isTransactionalLayout() {
    return false;
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHead() {
    return this.temporalGraphHeads;
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeads() {
    return this.temporalGraphHeads;
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeadsByLabel(String label) {
    return this.temporalGraphHeads.filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    throw new UnsupportedOperationException(
      "Converting a temporal graph to graph transactions is not supported yet.");
  }
}

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
package org.gradoop.flink.model.impl.layouts.gve.temporal;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * TODO: descriptions
 */
public class TemporalGraphCollectionLayoutFactory extends BaseFactory implements BaseLayoutFactory {

  public TemporalLayout fromDataSets(DataSet<TemporalGraphHead> graphHeads,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    return new TemporalLayout(graphHeads, vertices, edges);
  }

  public TemporalLayout fromCollections(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    Objects.requireNonNull(graphHeads, "Temporal graphHead collection is null.");
    Objects.requireNonNull(vertices, "Temporal vertex collection is null.");
    Objects.requireNonNull(edges, "Temporal edge collection is null.");

    return fromDataSets(
      createTemporalGraphHeadDataSet(graphHeads),
      createTemporalVertexDataSet(vertices),
      createTemporalEdgeDataSet(edges));
  }

  public TemporalLayout createEmptyCollection() {
    Collection<TemporalGraphHead> graphHeads = new ArrayList<>();
    Collection<TemporalVertex> vertices = new ArrayList<>();
    Collection<TemporalEdge> edges = new ArrayList<>();

    return fromCollections(graphHeads, vertices, edges);
  }

}

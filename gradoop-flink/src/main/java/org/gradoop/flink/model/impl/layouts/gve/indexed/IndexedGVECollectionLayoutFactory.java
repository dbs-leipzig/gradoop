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
package org.gradoop.flink.model.impl.layouts.gve.indexed;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating an {@link IndexedGVELayout} from given data.
 */
public class IndexedGVECollectionLayoutFactory extends GVECollectionLayoutFactory {

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromIndexedDataSets(
    Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices,
    Map<String, DataSet<EPGMEdge>> edges) {
    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);
    Objects.requireNonNull(edges);
    return new IndexedGVELayout(graphHeads, vertices, edges);
  }
}

/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.functions.ElementsToStats;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory for {@link BinningTemporalGraphStatistics}
 */
public class BinningTemporalGraphStatisticsFactory implements
  TemporalGraphStatisticsFactory<BinningTemporalGraphStatistics> {

  /**
   * Default size for a sample
   */
  static final int DEFAULT_SAMPLE_SIZE = 5000;

  @Override
  public BinningTemporalGraphStatistics fromGraph(TemporalGraph g) throws Exception {
    return fromGraphWithSampling(g, DEFAULT_SAMPLE_SIZE);
  }

  @Override
  public BinningTemporalGraphStatistics fromGraph(TemporalGraph g, Set<String> numericalProperties,
                                                  Set<String> categoricalProperties) throws Exception {
    return fromGraphWithSampling(g, DEFAULT_SAMPLE_SIZE, numericalProperties, categoricalProperties);
  }

  @Override
  public BinningTemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize)
    throws Exception {
    return fromGraphWithSampling(g, sampleSize, null, null);
  }

  @Override
  public BinningTemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize,
                                                              Set<String> numericalProperties,
                                                              Set<String> categoricalProperties)
    throws Exception {

    List<TemporalElementStats> vertexStats = Lists.newArrayList();
    g.getVertices()
      .groupBy(EPGMElement::getLabel)
      .reduceGroup(new ElementsToStats<>(numericalProperties, categoricalProperties))
      .output(new LocalCollectionOutputFormat<>(vertexStats));

    List<TemporalElementStats> edgeStats = Lists.newArrayList();
    g.getEdges()
      // do not replace this with the method reference!!!
      .groupBy(edge -> edge.getLabel())
      .reduceGroup(new ElementsToStats<>(numericalProperties, categoricalProperties))
      .output(new LocalCollectionOutputFormat<>(edgeStats));

    g.getConfig().getExecutionEnvironment().execute();

    HashSet<String> relevantProperties = null;
    // both only null, if all properties should be considered
    // (use empty lists to ignore all properties)
    if (numericalProperties != null && categoricalProperties != null) {
      relevantProperties = new HashSet<>();
      relevantProperties.addAll(numericalProperties);
      relevantProperties.addAll(categoricalProperties);
    }

    return new BinningTemporalGraphStatistics(vertexStats, edgeStats, relevantProperties);
  }
}

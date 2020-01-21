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
package org.gradoop.flink.model.impl.operators.keyedgrouping;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificAggregatorWrapper;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.UnlabeledGroupAggregatorWrapper;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificKeyFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Utilities for the {@link KeyedGrouping} implementation.<p>
 * This class provides functions used to convert arguments of the {@link Grouping} implementation to
 * key functions and aggregate functions.
 */
public final class KeyedGroupingUtils {

  /**
   * No instances of this class are needed.
   */
  private KeyedGroupingUtils() {
  }

  /**
   * Extract aggregate functions from a list of label groups.
   *
   * @param labelGroups A list of label groups.
   * @return A list of aggregate functions defined in those label groups.
   */
  public static List<AggregateFunction> asAggregateFunctions(List<LabelGroup> labelGroups) {
    LabelGroup defaultGroup = getDefaultGroupOrNull(labelGroups);
    List<AggregateFunction> functions;
    if (defaultGroup == null) {
      functions = new ArrayList<>();
      AtomicInteger id = new AtomicInteger(0);
      Set<String> allLabels = labelGroups.stream().map(LabelGroup::getGroupingLabel)
        .collect(Collectors.toSet());
      for (LabelGroup labelGroup : labelGroups) {
        final String currentLabel = labelGroup.getGroupingLabel();
        if (currentLabel.equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP) ||
          currentLabel.equals(Grouping.DEFAULT_EDGE_LABEL_GROUP)) {
          labelGroup.getAggregateFunctions().forEach(lga -> functions.add(
            new UnlabeledGroupAggregatorWrapper(allLabels, lga, (short) id.getAndIncrement())));
        } else {
          labelGroup.getAggregateFunctions().forEach(lga -> functions.add(
            new LabelSpecificAggregatorWrapper(currentLabel, lga, (short) id.getAndIncrement())));
        }
      }
    } else {
      functions = defaultGroup.getAggregateFunctions();
    }
    return functions;
  }

  /**
   * Convert label groups to key functions.
   *
   * @param useLabels   Flag used to indicate whether labels of elements should be used as grouping keys.
   * @param labelGroups The label groups to convert.
   * @param <T> The element type for the key function.
   * @return Key functions corresponding to those groups.
   */
  public static <T extends Element> List<KeyFunction<T, ?>> asKeyFunctions(
    boolean useLabels, List<LabelGroup> labelGroups) {
    LabelGroup defaultGroup = getDefaultGroupOrNull(labelGroups);
    List<KeyFunction<T, ?>> newKeys = new ArrayList<>();
    if (defaultGroup == null) {
      newKeys.add(asKeyFunction(useLabels, labelGroups));
    } else {
      defaultGroup.getPropertyKeys().forEach(k -> newKeys.add(GroupingKeys.property(k)));
      if (useLabels) {
        newKeys.add(GroupingKeys.label());
      }
    }
    return newKeys;
  }

  /**
   * Create a label-specific key function from a list of label groups.
   *
   * @param useLabels   Should labels be used for grouping?
   * @param labelGroups The label groups to convert.
   * @param <T> The element type for the key function.
   * @return The label-specific key function.
   */
  public static <T extends Element> LabelSpecificKeyFunction<T> asKeyFunction(
    boolean useLabels, List<LabelGroup> labelGroups) {
    Map<String, List<KeyFunctionWithDefaultValue<T, ?>>> keyFunctions = new HashMap<>();
    for (LabelGroup labelGroup : labelGroups) {
      final String groupingLabel = labelGroup.getGroupingLabel();
      if (keyFunctions.containsKey(groupingLabel)) {
        throw new UnsupportedOperationException("Duplicate grouping label: " + groupingLabel);
      }
      List<KeyFunctionWithDefaultValue<T, ?>> keysForLabel = new ArrayList<>();
      if ((groupingLabel.equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP) ||
        groupingLabel.equals(Grouping.DEFAULT_EDGE_LABEL_GROUP)) && useLabels) {
        keysForLabel.add(GroupingKeys.label());
      }
      labelGroup.getPropertyKeys().forEach(k -> keysForLabel.add(GroupingKeys.property(k)));
      keyFunctions.put(groupingLabel, keysForLabel);
    }
    Map<String, String> labelUpdateMap = labelGroups.stream()
      .collect(Collectors.toMap(LabelGroup::getGroupingLabel, LabelGroup::getGroupLabel));
    return new LabelSpecificKeyFunction<>(keyFunctions, labelUpdateMap);
  }

  /**
   * Create a new instance of the {@link KeyedGrouping} operator from a list of vertex and edge label groups.
   * This factory method also accepts global aggregators which will be applied to each label group.
   *
   * @param useVertexLabels         Group by vertex label.
   * @param useEdgeLabels           Group by edge label.
   * @param vertexLabelGroups       The vertex label groups.
   * @param edgeLabelGroups         The edge label groups.
   * @param globalVertexAggregators A list of aggregate functions applicable for all vertex groups.
   * @param globalEdgeAggregators   A list of aggregate functions applicable for all edge groups.
   * @param <G>  The graph head type.
   * @param <V>  The vertex type.
   * @param <E>  The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return A new instance of the grouping operator.
   */
  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> KeyedGrouping<G, V, E, LG, GC> createInstance(
    boolean useVertexLabels, boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups, List<LabelGroup> edgeLabelGroups,
    List<AggregateFunction> globalVertexAggregators, List<AggregateFunction> globalEdgeAggregators) {
    List<AggregateFunction> vertexAggregators = asAggregateFunctions(vertexLabelGroups);
    vertexAggregators.addAll(globalVertexAggregators);
    List<AggregateFunction> edgeAggregators = asAggregateFunctions(edgeLabelGroups);
    edgeAggregators.addAll(globalEdgeAggregators);
    return new KeyedGrouping<>(
      asKeyFunctions(useVertexLabels, vertexLabelGroups), vertexAggregators,
      asKeyFunctions(useEdgeLabels, edgeLabelGroups), edgeAggregators);
  }

  /**
   * Get the default label group or return {@code null} if other label groups exist in the list.<p>
   * This is used internally to check for label-specific grouping.
   *
   * @param labelGroups A list of label groups.
   * @return The default label group, if it is the only label group and {@code null} otherwise
   */
  private static LabelGroup getDefaultGroupOrNull(List<LabelGroup> labelGroups) {
    if (labelGroups.size() != 1) {
      return null;
    } else {
      LabelGroup labelGroup = labelGroups.get(0);
      if (!(labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_EDGE_LABEL_GROUP) ||
        labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP))) {
        return null;
      }
      return labelGroup;
    }
  }
}

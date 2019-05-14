/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for label specific grouping.
 * <p>
 * The output of that mapper is {@link VertexGroupItem} that contains the vertex id,
 * vertex label, vertex group properties and vertex aggregate properties.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildVertexGroupItem extends BuildGroupItemBase implements
  FlatMapFunction<Vertex, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * True, iff vertices without labels will be converted to individual groups/ supervertices.
   * False, iff vertices without labels will be collapsed into a single group/ supervertice.
   */
  private final boolean keepVertices;

  /**
   * Creates map function
   *
   * @param useLabel          true, if label shall be considered
   * @param vertexLabelGroups stores grouping properties for vertex labels
   * @param keepVertices      true, if vertices without labels create new groups
   */
  public BuildVertexGroupItem(boolean useLabel, List<LabelGroup> vertexLabelGroups,
    boolean keepVertices) {
    super(useLabel, vertexLabelGroups);

    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);
    this.reuseVertexGroupItem.setTemporaryLabel(false);
    this.keepVertices = keepVertices;
  }

  /**
   * TODO temporary, delete
   */
  static int NO_LABEL = 0;

  /**
   * Function that just returns the last element without aggregating anything.
   */
  private static class LastElement extends BaseAggregateFunction {

    /**
     * Creates a new instance.
     *
     * @param aggregatePropertyKey property to aggregate
     */
    LastElement(String aggregatePropertyKey) {
      super(aggregatePropertyKey);
    }

    @Override
    public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
      aggregate = increment;
      return aggregate;
    }

    @Override
    public PropertyValue getIncrement(EPGMElement element) {
      return element.getPropertyValue(getAggregatePropertyKey());
    }
  }

  @Override
  public void flatMap(Vertex vertex, Collector<VertexGroupItem> collector) throws Exception {
    boolean usedVertexLabelGroup = false;

    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setTemporaryLabel(false);

   /* if (keepVertices && vertex.getLabel().isEmpty()) {

      // TODO need to set new, unique label somehow
      // causes creation of a new, individual group for this vertex
      // which causes the instantiation of a new super vertex
      vertex.setLabel("noLabel_" + NO_LABEL++);

      LabelGroup defaultLabelGroup = getDefaultLabelGroup();

      ArrayList<String> propertyKeys = new ArrayList<>();

      // Copy aggregate functions set by user.
      ArrayList<AggregateFunction> aggregators =
        new ArrayList<>(defaultLabelGroup.getAggregateFunctions());

      // Create aggregate functions for every property to copy.
      vertex.getPropertyKeys().forEach(key -> aggregators.add(new LastElement(key)));

      LabelGroup labelGroup = new LabelGroup(defaultLabelGroup.getGroupingLabel(),
        vertex.getLabel()/* <-- is only used in setGroupItem, if useLabels is not set *///,
   /*     propertyKeys, aggregators);

      setGroupItem(reuseVertexGroupItem, vertex, labelGroup);
      reuseVertexGroupItem.setTemporaryLabel(true);

      collector.collect(reuseVertexGroupItem);
      return;
    }*/

    // check if vertex shall be grouped by a special set of keys
    for (LabelGroup vertexLabelGroup : getLabelGroups()) {
      if (vertexLabelGroup.getGroupingLabel().equals(vertex.getLabel())) {
        usedVertexLabelGroup = true;
        setGroupItem(reuseVertexGroupItem, vertex, vertexLabelGroup);
        collector.collect(reuseVertexGroupItem);
      }
    }

    // standard grouping case
    if (!usedVertexLabelGroup) {
      setGroupItem(reuseVertexGroupItem, vertex, getDefaultLabelGroup());
      collector.collect(reuseVertexGroupItem);
    }
  }
}

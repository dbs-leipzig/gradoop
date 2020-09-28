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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.dummy;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatisticsFactory;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.Optional;

/**
 * A dummy graph statistics where every relevant value is set to 1
 */
public class DummyTemporalGraphStatistics extends TemporalGraphStatistics {
  @Override
  public TemporalGraphStatisticsFactory getFactory() {
    return null;
  }

  @Override
  public long getVertexCount(String label) {
    return 1;
  }

  @Override
  public long getVertexCount() {
    return 1;
  }

  @Override
  public long getEdgeCount(String label) {
    return 1;
  }

  @Override
  public long getEdgeCount() {
    return 1;
  }

  @Override
  public long getDistinctSourceVertexCount(String edgeLabel) {
    return 1;
  }

  @Override
  public long getDistinctSourceVertexCount() {
    return 1;
  }

  @Override
  public long getDistinctTargetVertexCount() {
    return 1;
  }

  @Override
  public long getDistinctTargetVertexCount(String edgeLabel) {
    return 1;
  }

  @Override
  public double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                     TimeSelector.TimeField field1, Comparator comp, Long value) {
    return 1;
  }

  @Override
  public double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                     TimeSelector.TimeField field1, Comparator comp, ElementType type2,
                                     Optional<String> label2, TimeSelector.TimeField field2) {
    return 1;
  }

  @Override
  public double estimateDurationProb(ElementType type, Optional<String> label, Comparator comp,
                                     boolean transaction, Long value) {
    return 1;
  }

  @Override
  public double estimateDurationProb(ElementType type1, Optional<String> label1, boolean transaction1,
                                     Comparator comp, ElementType type2, Optional<String> label2,
                                     boolean transaction2) {
    return 1;
  }

  @Override
  public double estimatePropertyProb(ElementType type, Optional<String> label, String property,
                                     Comparator comp, PropertyValue value) {
    return 1;
  }

  @Override
  public double estimatePropertyProb(ElementType type1, Optional<String> label1, String property1,
                                     Comparator comp, ElementType type2, Optional<String> label2,
                                     String property2) {
    return 1;
  }

}

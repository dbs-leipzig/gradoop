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
package org.gradoop.flink.model.impl.operators.grouping;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class GroupingBuilderTest {

  /**
   * Tests {@link Grouping.GroupingBuilder#build()}, expects a correct exception when not
   * grouping by anything.
   */
  @Test(expected = IllegalStateException.class)
  public void testGroupByNothingError() {

    new Grouping.GroupingBuilder()
      .build();
  }

  /**
   * Tests {@link Grouping.GroupingBuilder#build()}, expects a correct exception when not
   * grouping by anything.
   */
  @Test(expected = IllegalStateException.class)
  public void testGroupByNothingError2() {

    new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .build();
  }

  /**
   * Tests {@link Grouping.GroupingBuilder#build()}, expects a correct exception when no
   * {@link GroupingStrategy} was set.
   */
  @Test(expected = IllegalStateException.class)
  public void testNoStrategySetError() {

    new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .build();
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when a strategy is set and
   * when only grouping by vertex labels.
   */
  @Test
  public void testStrategySet() {

    new Grouping.GroupingBuilder()
      .useVertexLabel(true)
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .build();
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when only grouping by
   * one property.
   */
  @Test
  public void testAtLeastOneProperty() {

    new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addVertexGroupingKey("")
      .build();
  }

  /**
   * Tests successful call to {@link Grouping.GroupingBuilder#build()} when only using specific
   * label grouping.
   */
  @Test
  public void testLabelSpecificGroup() {

    new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addVertexLabelGroup("", ImmutableList.of())
      .build();
  }

}

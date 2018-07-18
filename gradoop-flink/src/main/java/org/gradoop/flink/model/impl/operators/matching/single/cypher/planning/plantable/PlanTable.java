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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * A data structure to manage multiple query plans. Each {@link QueryPlan} is represented by a
 * {@link PlanTableEntry}.
 */
public class PlanTable implements Iterable<PlanTableEntry> {
  /**
   * Entries of this plan table
   */
  private List<PlanTableEntry> planTableEntries;

  /**
   * Creates a new plan table
   */
  public PlanTable() {
    planTableEntries = new ArrayList<>();
  }

  /**
   * Adds a {@link PlanTableEntry} to the plan table.
   *
   * @param planTableEntry entry
   */
  public void add(PlanTableEntry planTableEntry) {
    planTableEntries.add(planTableEntry);
  }

  /**
   * Returns the entry at the specified position.
   *
   * @param index position in the plan table
   * @return entry at specified position
   */
  public PlanTableEntry get(int index) {
    return planTableEntries.get(index);
  }

  /**
   * Removes all entries from the table whose query plans are covered by the query plan wrapped by
   * the specified entry.
   *
   * @param planTableEntry entry
   */
  public void removeCoveredBy(PlanTableEntry planTableEntry) {
    planTableEntries = planTableEntries.stream()
      .filter(e -> !planTableEntry.getProcessedVariables().containsAll(e.getProcessedVariables()))
      .collect(Collectors.toList());
  }

  /**
   * Returns the entry that represents the query plan with the minimum among all plans stored in
   * this table.
   *
   * @return query plan with minimum cost
   */
  public PlanTableEntry min() {
    return planTableEntries.stream()
      .sorted(Comparator.comparingLong(PlanTableEntry::getEstimatedCardinality))
      .findFirst()
      .orElseThrow(NoSuchElementException::new);
  }

  /**
   * Returns the number of entries in the table.
   *
   * @return number of entries
   */
  public int size() {
    return planTableEntries.size();
  }

  @Override
  public Iterator<PlanTableEntry> iterator() {
    return planTableEntries.iterator();
  }
}

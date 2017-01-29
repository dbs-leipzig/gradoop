/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * A data structure to manage multiple query plans. Each query plan is represented by a
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
   * Removes all entries that are processed by the given {@link PlanTableEntry}.
   *
   * @param planTableEntry entry
   */
  public void removeProcessedBy(PlanTableEntry planTableEntry) {
    planTableEntries = planTableEntries.stream()
      .filter(e -> !planTableEntry.getProcessedVariables().containsAll(e.getProcessedVariables()))
      .collect(Collectors.toList());
  }

  /**
   * Returns the plan entry that represents the query plan with the minimum cost in this table.
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

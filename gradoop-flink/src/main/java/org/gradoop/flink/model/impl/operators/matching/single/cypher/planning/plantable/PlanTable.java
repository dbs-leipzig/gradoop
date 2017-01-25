package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class PlanTable implements Iterable<PlanTableEntry> {
  private List<PlanTableEntry> planTableEntries;

  public PlanTable() {
    planTableEntries = new ArrayList<>();
  }

  public void add(PlanTableEntry planTableEntry) {
    planTableEntries.add(planTableEntry);
  }

  public PlanTableEntry get(int index) {
    return planTableEntries.get(index);
  }

  public void removeProcessedBy(PlanTableEntry entry) {
    planTableEntries = planTableEntries.stream()
      .filter(e -> !entry.getProcessedVariables().containsAll(e.getProcessedVariables()))
      .collect(Collectors.toList());
  }

  public PlanTableEntry min() {
    return planTableEntries.stream()
      .sorted(Comparator.comparingLong(PlanTableEntry::getEstimatedCardinality))
      .findFirst().get();
  }

  public int size() {
    return planTableEntries.size();
  }

  @Override
  public Iterator<PlanTableEntry> iterator() {
    return planTableEntries.iterator();
  }
}

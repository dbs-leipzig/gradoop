package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.plantable;

import java.util.*;
import java.util.stream.Collectors;

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
                .min(Comparator.comparingLong(PlanTableEntry::getEstimatedCardinality))
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

package org.gradoop.temporal.model.impl.operators.matching.common.statistics;

import org.gradoop.temporal.model.impl.TemporalGraph;

public interface TemporalGraphStatisticsFactory<T extends TemporalGraphStatistics> {

    T fromGraph(TemporalGraph g);

    T fromGraphWithSampling(TemporalGraph g, int sampleSize);
}

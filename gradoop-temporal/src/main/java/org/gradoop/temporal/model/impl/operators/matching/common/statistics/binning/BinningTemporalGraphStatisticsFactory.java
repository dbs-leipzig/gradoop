package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.functions.ElementsToStats;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.ReservoirSampler;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;

public class BinningTemporalGraphStatisticsFactory implements
        TemporalGraphStatisticsFactory<BinningTemporalGraphStatistics> {
    @Override
    public BinningTemporalGraphStatistics fromGraph(TemporalGraph g) {
        return null;
    }

    @Override
    public BinningTemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize)  {
        try {
            List<TemporalElementStats> vertexStats = g.getVertices()
                    .groupBy(EPGMElement::getLabel)
                    .reduceGroup(new ElementsToStats<TemporalVertex>())
                    .collect();
            List<TemporalElementStats> edgeStats = g.getEdges()
                    // do not replace this with the method reference!!!
                    .groupBy(edge -> edge.getLabel())
                    .reduceGroup(new ElementsToStats<TemporalEdge>())
                    .collect();
            return new BinningTemporalGraphStatistics(vertexStats, edgeStats);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

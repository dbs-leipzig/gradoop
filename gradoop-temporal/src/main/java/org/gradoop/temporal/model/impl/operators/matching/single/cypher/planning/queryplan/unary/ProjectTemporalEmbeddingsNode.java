package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.ProjectTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unary node that wraps a {@link ProjectTemporalEmbeddings} operator.
 */
public class ProjectTemporalEmbeddingsNode extends UnaryNode implements ProjectionNode {
    /**
     * Property columns that are taken over to the output embedding
     */
    private final List<Integer> whiteListColumns;
    /**
     * Property keys used for projection
     */
    private final List<Pair<String, String>> projectionKeys;

    /**
     * Creates new node.
     *
     * @param childNode input plan node
     * @param projectionKeys property keys whose associated values are projected to the output
     */
    public ProjectTemporalEmbeddingsNode(PlanNode childNode, List<Pair<String, String>> projectionKeys) {
        super(childNode);
        this.projectionKeys = projectionKeys;
        EmbeddingTPGMMetaData childMetaData = childNode.getEmbeddingMetaData();

        // compute columns of projected properties
        whiteListColumns = projectionKeys.stream()
                .map(pair -> childMetaData.getPropertyColumn(pair.getLeft(), pair.getRight()))
                .collect(Collectors.toList());
    }

    @Override
    public DataSet<EmbeddingTPGM> execute() {
        ProjectTemporalEmbeddings op =
                new ProjectTemporalEmbeddings(getChildNode().execute(), whiteListColumns);
        op.setName(toString());
        return op.evaluate();
    }

    @Override
    protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
        final EmbeddingTPGMMetaData childMetaData = getChildNode().getEmbeddingMetaData();

        projectionKeys.sort(Comparator.comparingInt(key ->
                childMetaData.getPropertyColumn(key.getLeft(), key.getRight())));

        EmbeddingTPGMMetaData embeddingMetaData = new EmbeddingTPGMMetaData();

        childMetaData.getVariables().forEach(var -> embeddingMetaData.setEntryColumn(
                var, childMetaData.getEntryType(var), childMetaData.getEntryColumn(var)));


        childMetaData.getTimeDataMapping().keySet().forEach(var -> embeddingMetaData.setTimeColumn(
                var, childMetaData.getTimeColumn(var)));



        IntStream.range(0, projectionKeys.size()).forEach(i ->
                embeddingMetaData.setPropertyColumn(
                        projectionKeys.get(i).getLeft(), projectionKeys.get(i).getRight(), i));

        return embeddingMetaData;
    }


}

package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;

/**
 * Utility class to convert an element ({@link TemporalVertex} and {@link TemporalEdge} into an
 * {@link EmbeddingTPGM}.
 * Practically identical to
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory}
 * but creating TPGMEmbeddings from TPGM elements
 */
public class EmbeddingTPGMFactory {

    /**
     * Converts a {@link TemporalVertex} into an {@link EmbeddingTPGM}.
     *
     * The resulting embedding has one entry containing the vertex id and one entry for each property
     * value associated with the specified property keys (ordered by list order). Note that missing
     * property values are represented by a {@link PropertyValue#NULL_VALUE}.
     * Furthermore, the time data of each element ({tx_from, tx_to, valid_from, valid_to}) is stored
     * in the embedding
     *
     * @param vertex vertex to create embedding from
     * @param propertyKeys properties that will be stored in the embedding
     * @return Embedding
     */
    public static EmbeddingTPGM fromVertex(TemporalVertex vertex, List<String> propertyKeys){
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(vertex.getId(), project(vertex, propertyKeys),
                vertex.getTxFrom(), vertex.getTxTo(), vertex.getValidFrom(), vertex.getValidTo());
        return embedding;
    }

    /**
     * Converts an {@link TemporalEdge} into an {@link EmbeddingTPGM}.
     *
     * The resulting embedding has three entries containing the source vertex id, the edge id and the
     * target vertex id. Furthermore, the embedding has one entry for each property value associated
     * with the specified property keys (ordered by list order). Note that missing property values are
     * represented by a {@link PropertyValue#NULL_VALUE}.
     * Additionally, the time data of the edge ({tx_from, tx_to, valid_from, valid_to}) are stored
     * in the embedding.
     *
     * @param edge edge to create embedding from
     * @param propertyKeys properties that will be stored in the embedding
     * @param isLoop indicates if the edges is a loop
     * @return Embedding
     */
    public static EmbeddingTPGM fromEdge(TemporalEdge edge, List<String> propertyKeys, boolean isLoop){
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        if(isLoop){
            embedding.addAll(edge.getSourceId(), edge.getId());
        }
        else{
            embedding.addAll(edge.getSourceId(), edge.getId(), edge.getTargetId());
        }
        embedding.addPropertyValues(project(edge,propertyKeys));
        embedding.addTimeData(edge.getTxFrom(), edge.getTxTo(), edge.getValidFrom(), edge.getValidTo());
        return embedding;
    }

    /**
     * Converts an {@link TripleTPGM} into an {@link EmbeddingTPGM}.
     *
     * The resulting embedding has two or three entries containing the source vertex id, the edge id
     * and the target vertex id. Furthermore, the embedding has one entry for each property value
     * associated with the specified property keys (ordered by source properties, edge properties,
     * target properties in list order). Note that missing property values are represented
     * by a {@link PropertyValue#NULL_VALUE}.
     * Additionally, the time data ({tx_from, tx_to, valid_from, valid_to}) is stored for source,
     * edge and target in the embedding.
     *
     * @param triple triple to create embedding from
     * @param sourcePropertyKeys source properties that will be stored in the embedding
     * @param edgePropertyKeys edge properties that will be stored in the embedding
     * @param targetPropertyKeys target properties that will be stored in the embedding
     * @param sourceVertexVariable variable of the source vertex
     * @param targetVertexVariable variable of the target vertex
     * @return Embedding
     */
    public static EmbeddingTPGM fromTriple(TripleTPGM triple, List<String> sourcePropertyKeys,
                                           List<String> edgePropertyKeys, List<String> targetPropertyKeys, String sourceVertexVariable,
                                           String targetVertexVariable){
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        TemporalVertex source = triple.getSourceVertex();
        embedding.add(
                source.getId(), project(source, sourcePropertyKeys),
                source.getTxFrom(), source.getTxTo(), source.getValidFrom(), source.getValidTo()
        );

        TemporalEdge edge = triple.getEdge();
        embedding.add(
                edge.getId(), project(edge, edgePropertyKeys),
                edge.getTxFrom(), edge.getTxTo(), edge.getValidFrom(), edge.getValidTo()
        );

        if (sourceVertexVariable.equals(targetVertexVariable)) {
            return embedding;
        }

        TemporalVertex target = triple.getTargetVertex();
        embedding.add(
                target.getId(), project(target, targetPropertyKeys),
                target.getTxFrom(), target.getTxTo(), target.getValidFrom(), target.getValidTo()
        );

        return embedding;
    }

    /**
     * Projects the elements properties into a list of property values. Only those properties
     * specified by their key will be kept. Properties that are specified but not present at the
     * element will be adopted as {@link PropertyValue#NULL_VALUE}.
     * Copied from
     * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory}
     *
     * @param element element of which the properties will be projected
     * @param propertyKeys properties that will be projected from the specified element
     * @return projected property values
     */
    private static PropertyValue[] project(GraphElement element, List<String> propertyKeys) {
        PropertyValue[] propertyValues = new PropertyValue[propertyKeys.size()];
        int i = 0;
        for (String propertyKey : propertyKeys) {
            if (propertyKey.equals("__label__")) {
                propertyValues[i++] = PropertyValue.create(element.getLabel());
            } else {
                propertyValues[i++] = element.hasProperty(propertyKey) ?
                        element.getPropertyValue(propertyKey) : PropertyValue.NULL_VALUE;
            }
        }
        return propertyValues;
    }

}

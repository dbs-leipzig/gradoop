package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryComparableTPGM;
import org.s1ck.gdl.model.comparables.ElementSelector;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Wraps an {@link org.s1ck.gdl.model.comparables.ElementSelector}
 */
public class ElementSelectorComparable extends QueryComparableTPGM {
    /**
     * Holds the wrapped ElementSelector
     */
    private final ElementSelector elementSelector;

    /**
     * Creates a new Wrapper
     *
     * @param elementSelector the wrapped ElementSelectorComparable
     */
    public ElementSelectorComparable(ElementSelector elementSelector) {
        this.elementSelector = elementSelector;
    }

    /**
     * Returns a property values that wraps the elements id
     *
     * @param embedding the embedding holding the data
     * @param metaData meta data describing the embedding
     * @return property value of element id
     */
    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        int column = metaData.getEntryColumn(elementSelector.getVariable());
        return PropertyValue.create(embedding.getId(column));
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        return PropertyValue.create(element.getId());
    }

    @Override
    public Set<String> getPropertyKeys(String variable) {
        return new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ElementSelectorComparable that = (ElementSelectorComparable) o;

        return Objects.equals(elementSelector, that.elementSelector);
    }

    @Override
    public int hashCode() {
        return elementSelector != null ? elementSelector.hashCode() : 0;
    }
}


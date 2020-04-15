package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.MergeTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * This operator builds the cartesian product of two disjunct embeddings and combines them into a
 * single embedding.
 *
 * The result is always a new embedding with the following constraints.
 *
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>all properties from the right side are appended to the proeprties of the left side
 * </ul>
 */
public class CartesianProduct implements PhysicalTPGMOperator {

    /**
     * Left side embeddings
     */
    private final DataSet<EmbeddingTPGM> left;
    /**
     * Right side embeddings
     */
    private final DataSet<EmbeddingTPGM> right;
    /**
     * Number of columns in the right embedding.
     */
    private final int rightColumns;
    /**
     * Columns that represent vertices in the left embedding which need to be distinct
     */
    private final List<Integer> distinctVertexColumnsLeft;
    /**
     * Columns that represent vertices in the right embedding which need to be distinct
     */
    private final List<Integer> distinctVertexColumnsRight;
    /**
     * Columns that represent edges in the left embedding which need to be distinct
     */
    private final List<Integer> distinctEdgeColumnsLeft;
    /**
     * Columns that represent edges in the right embedding which need to be distinct
     */
    private final List<Integer> distinctEdgeColumnsRight;
    /**
     * Flink cross Hint
     */
    private final CrossOperatorBase.CrossHint crossHint;
    /**
     * Operator name used for Flink operator naming
     */
    private String name;

    /**
     * New value join operator
     *
     * @param left left hand side data set
     * @param right right hand side data set
     * @param rightColumns size of the right embedding
     * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
     * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
     * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
     * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
     * @param crossHint cross hint
     */
    public CartesianProduct(
            DataSet<EmbeddingTPGM> left,
            DataSet<EmbeddingTPGM> right,
            int rightColumns,
            List<Integer> distinctVertexColumnsLeft,
            List<Integer> distinctVertexColumnsRight,
            List<Integer> distinctEdgeColumnsLeft,
            List<Integer> distinctEdgeColumnsRight,
            CrossOperatorBase.CrossHint crossHint) {

        this.left = left;
        this.right = right;
        this.rightColumns = rightColumns;
        this.distinctVertexColumnsLeft = distinctVertexColumnsLeft;
        this.distinctVertexColumnsRight = distinctVertexColumnsRight;
        this.distinctEdgeColumnsLeft = distinctEdgeColumnsLeft;
        this.distinctEdgeColumnsRight = distinctEdgeColumnsRight;
        this.crossHint = crossHint;
        this.setName("CartesianProduct");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return left.cross(right)
                .flatMap(new MergeTemporalEmbeddings(
                        rightColumns, Lists.newArrayListWithCapacity(0),
                        distinctVertexColumnsLeft,
                        distinctVertexColumnsRight,
                        distinctEdgeColumnsLeft,
                        distinctEdgeColumnsRight
                ))
                .name(getName());
    }

    @Override
    public void setName(String newName) {
        this.name = newName;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
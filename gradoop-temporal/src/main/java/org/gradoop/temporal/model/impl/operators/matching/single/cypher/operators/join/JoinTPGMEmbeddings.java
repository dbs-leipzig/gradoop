package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;


import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractTimeJoinColumns;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractJoinColumns;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.MergeTemporalEmbeddings;

import java.util.Collections;
import java.util.List;

/**
 * Joins two temporal embeddings at given columns and checks for vertex/edge isomorphism/homomorphism.
 *
 * The result is always a new embedding with the following constraints.
 *
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>duplicate fields are removed, i.e., the join columns are stored once in the result</li>
 * <li>all properties and time data from the right side are appended to the proeprties of the left
 * side,
 *     <em>no</em> deduplication is performed</li>
 * </ul>
 */
public class JoinTPGMEmbeddings implements PhysicalTPGMOperator {

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
     * Left side join columns
     */
    private final List<Integer> leftJoinColumns;
    /**
     * Right side join columns
     */
    private final List<Integer> rightJoinColumns;
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
     * Flink join Hint
     */
    private final JoinOperatorBase.JoinHint joinHint;

    /**
     * Operator name
     */
    private String name;

    /**
     * Instantiates a new join operator.
     *
     * @param left embeddings of the left side of the join
     * @param right embeddings of the right side of the join
     * @param rightColumns number of columns in the right side of the join
     * @param leftJoinColumn join column left side
     * @param rightJoinColumn join column right side
     */
    public JoinTPGMEmbeddings(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                          int rightColumns,
                          int leftJoinColumn, int rightJoinColumn) {
        this(left, right, rightColumns,
                Collections.singletonList(leftJoinColumn), Collections.singletonList(rightJoinColumn));
    }

    /**
     * Instantiates a new join operator.
     *
     * @param left embeddings of the left side of the join
     * @param right embeddings of the right side of the join
     * @param rightColumns number of columns in the right side of the join
     * @param leftJoinColumns specifies the join columns of the left side
     * @param rightJoinColumns specifies the join columns of the right side
     */
    public JoinTPGMEmbeddings(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                          int rightColumns,
                          List<Integer> leftJoinColumns, List<Integer> rightJoinColumns) {
        this(left, right, rightColumns,
                leftJoinColumns, rightJoinColumns,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Instantiates a new join operator.
     *
     * @param left embeddings of the left side of the join
     * @param right embeddings of the right side of the join
     * @param rightColumns number of columns in the right side of the join
     * @param leftJoinColumns specifies the join columns of the left side
     * @param rightJoinColumns specifies the join columns of the right side
     * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
     * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
     * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
     * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
     */
    public JoinTPGMEmbeddings(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                          int rightColumns,
                          List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
                          List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
                          List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight) {
        this(left, right, rightColumns,
                leftJoinColumns, rightJoinColumns,
                distinctVertexColumnsLeft, distinctVertexColumnsRight,
                distinctEdgeColumnsLeft, distinctEdgeColumnsRight,
                JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
    }

    /**
     * Instantiates a new join operator.
     *
     * @param left embeddings of the left side of the join
     * @param right embeddings of the right side of the join
     * @param rightColumns number of columns in the right side of the join
     * @param leftJoinColumns specifies the join columns of the left side
     * @param rightJoinColumns specifies the join columns of the right side
     * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
     * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
     * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
     * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
     * @param joinHint join strategy
     */
    public JoinTPGMEmbeddings(DataSet<EmbeddingTPGM> left, DataSet<EmbeddingTPGM> right,
                          int rightColumns,
                          List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
                          List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
                          List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight,
                          JoinOperatorBase.JoinHint joinHint) {
        this.left                       = left;
        this.right                      = right;
        this.rightColumns               = rightColumns;
        this.leftJoinColumns            = leftJoinColumns;
        this.rightJoinColumns           = rightJoinColumns;
        this.distinctVertexColumnsLeft  = distinctVertexColumnsLeft;
        this.distinctVertexColumnsRight = distinctVertexColumnsRight;
        this.distinctEdgeColumnsLeft    = distinctEdgeColumnsLeft;
        this.distinctEdgeColumnsRight   = distinctEdgeColumnsRight;
        this.joinHint                   = joinHint;
        this.setName("JoinEmbeddingTPGM");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return left.join(right, joinHint)
                .where(new ExtractJoinColumns(leftJoinColumns))
                .equalTo(new ExtractJoinColumns(rightJoinColumns))
                .with(new MergeTemporalEmbeddings(rightColumns, rightJoinColumns,
                        distinctVertexColumnsLeft, distinctVertexColumnsRight,
                        distinctEdgeColumnsLeft, distinctEdgeColumnsRight))
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


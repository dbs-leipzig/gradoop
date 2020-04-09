package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * This class represents an Embedding, an ordered List of Embedding Entries. Every entry is
 * either a reference to a single Edge or Vertex, or a
 * path (Edge, Vertex, Edge, Vertex, ..., Edge).
 * The reference is stored via the elements ID. Additionally the embedding can store an ordered
 * list of PropertyValues.
 * Furthermore, for every vertex and edge, temporal data of the form {tx_from, tx_to, val_from, val_to}
 * as defined by the TPGM can be stored.
 */
public class EmbeddingTPGM extends org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding 
        {

    /**
     * Holds all time fields in the form (tx_from, tx_to, val_from, val_to)
     */
    private byte[] timeData;

    /**
     * Creates an empty embedding
     */
    public EmbeddingTPGM(){
        super();
        timeData = new byte[0];
    }

    /**
     * Creates an Embedding with the given data
     * @param idData id data stored in a byte array
     * @param propertyData Properties stored in internal byte array format
     * @param idListData IdLists stored in internal byte array format
     * @param timeData time fields stored in internal byte array format
     */
    public EmbeddingTPGM(byte[] idData, byte[] propertyData, byte[] idListData, byte[] timeData){
        super(idData, propertyData, idListData);
        this.timeData = timeData;
    }

    // ---------------------------------------------------------------------------------------------
    //  Time data Handling
    // ---------------------------------------------------------------------------------------------

    /**
     * Appends time information of a graph element to the embedding
     * @param tx_f the tx_from value
     * @param tx_t the tx_to value
     * @param val_f the val_from value
     * @param val_t the val_to value
     */
    public void addTimeData(Long tx_f, Long tx_t, Long val_f, Long val_t){
        if(tx_f > tx_t || val_f > val_t || tx_f < 0 || tx_t < 0 || val_f < 0 || val_t < 0){
            throw new IllegalArgumentException("to must be >= from, time fields are not negative");
        }
        byte[] newTimeData = new byte[timeData.length+ 4*Long.BYTES];
        System.arraycopy(timeData, 0, newTimeData,0, timeData.length);
        byte[] additionalData = longsToByteArray(tx_f, tx_t, val_f, val_t);
        System.arraycopy(additionalData, 0, newTimeData, timeData.length, 4*Long.BYTES);
        timeData = newTimeData;
    }
    

    /**
     * Appends a GradoopId as well as the specified properties and its time fields to the embedding.
     * The id, the properties and the time data will be added to the end of the corresponding Lists
     * @param id that will be added to the embedding
     * @param properties list of property values
     * @param tx_from transaction time from for the element
     * @param tx_to transaction time to for the element
     * @param val_from validation time from for the element
     * @param val_to validation time from to the element
     */
    public void add(GradoopId id, PropertyValue[] properties, long tx_from, long tx_to,
                    long val_from, long val_to){
        super.add(id, properties);
        addTimeData(tx_from, tx_to, val_from, val_to);
    }

    /**
     * Returns the time values ({tx_from, tx_to, val_from, val_to}) stored at the specified position
     * @param column the position the time values are stored at
     * @return time values ({tx_from, tx_to, val_from, val_to}) at that position
     */
    public Long[] getTimes(int column){
        return byteArrayToLongs(getRawTimeEntry(column));
    }

    /**
     * Returns the internal representation of the TimeEntry stored at the specified position
     * @param column the position the entry is stored at
     * @return internal representation of the entry
     */
    public byte[] getRawTimeEntry(int column){
        int offset = 4*Long.BYTES*column;
        if(offset <0 || offset >= timeData.length ){
            throw new IndexOutOfBoundsException();
        }
        return ArrayUtils.subarray(timeData, offset, offset+4*Long.BYTES);
    }

    /**
     * Returns number of time entries of the form {tx_from, tx_to, val_from, val_to}
     * @return number of time entries
     */
    private int numberOfTimeEntries(){
        return timeData.length/(4*Long.BYTES);
    }

    /**
     * Converts a byte array to an array of Longs
     * @param source a byte array with size dividable by Long.BYTES(=8)
     * @return Long array represented by the byte array
     */
    private Long[] byteArrayToLongs(byte[] source){
        Long[] ret = new Long[source.length/Long.BYTES];
        for(int i=0; i<ret.length; i++){
            byte[] nextLong = ArrayUtils.subarray(source, i*Long.BYTES, (i+1)*Long.BYTES);
            ret[i] = byteArrayToLong(nextLong);
        }
        return ret;
    }

    /**
     * Converts byte array into long
     * @param bts byte array of size Long.BYTES (=8)
     * @return long represented by the byte array
     */
    private Long byteArrayToLong(byte[] bts){
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.put(bts);
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    /**
     * Converts a long array to a byte array
     * @param source the long array to convert
     * @return byte array encoding the long array
     */
    private byte[] longsToByteArray(Long...source){
        byte[] res = new byte[source.length*Long.BYTES];
        for(int i=0; i<source.length; i++){
            byte[] bts = longToByteArray(source[i]);
            System.arraycopy(bts, 0, res, i*Long.BYTES, Long.BYTES);
        }
        return res;
    }

    /**
     * Converts a long to a byte array
     * @param l the long to convert
     * @return the byte array encoding the long
     */
    private byte[] longToByteArray(Long l){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            dos.writeLong(l);
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }

    // ---------------------------------------------------------------------------------------------
    //  Internal State
    // ---------------------------------------------------------------------------------------------

    /**
     * Returns the internal representation of the stored time data
     * @return internal representation of the stored time data
     */
    public byte[] getTimeData(){
        return this.timeData;
    }

    /**
     * sets the time data
     * @param timeData byte representation of n*4 Longs
     */
    public void setTimeData(byte[] timeData){
        if ((timeData.length % (4*Long.BYTES))!=0){
            throw new IllegalArgumentException("no byte representation of time data!");
        }
        // no other check for correctness here, for the sake of efficiency...
        this.timeData = timeData;
    }

    /**
     * Projects the stored Properties. Only the white-listed properties will be kept
     * Method is already present in Embedding, but must be adjusted to new return type EmbeddingTPGM
     * @param propertyWhiteList list of property indices
     * @return Embedding (TPGM!) with the projected property list
     */
    public EmbeddingTPGM project(List<Integer> propertyWhiteList){
        /*//could be implemented like:
        Embedding e = new Embedding(getIdData(), getPropertyData(), getIdListData())
                .project(propertyWhiteList);
        return new EmbeddingTPGM(e.getIdData(), e.getPropertyData(), e.getIdListData(), timeData);
        // but more efficient to copy the implementation in Embedding:*/
        byte[] newPropertyData = new byte[0];
        for (int index : propertyWhiteList) {
            newPropertyData = ArrayUtils.addAll(newPropertyData, getRawProperty(index));
        }

        return new EmbeddingTPGM(getIdData(), newPropertyData, getIdListData(), timeData);
    }

    /**
     * Reverses the order of the entries stored in the embedding.
     * The order of the properties will stay untouched.
     * Method is already present in Embedding, but must be adjusted to new return type EmbeddingTPGM
     * @return  A new Embedding (TPGM!) with reversed entry order
     */
    public EmbeddingTPGM reverse(){
        /*//could be implemented like
        Embedding e = new Embedding(getIdData(), getPropertyData(), getIdListData()).reverse();
        return new EmbeddingTPGM(e.getIdData(), e.getPropertyData(), e.getIdListData(), timeData);
        //but more efficient to copy the implementation in Embedding: */
        byte[] newIdData = new byte[getIdData().length];

        for (int i = size() - 1; i >= 0; i--) {
            System.arraycopy(
                    getRawIdEntry(i), 0,
                    newIdData,  (size() - 1 - i) * ID_ENTRY_SIZE,
                    ID_ENTRY_SIZE
            );
        }

        return new EmbeddingTPGM(newIdData, getPropertyData(), getIdListData(), timeData);
    }

    // ---------------------------------------------------------------------------------------------
    //  Serialisation
    // ---------------------------------------------------------------------------------------------

    @Override
    public void copyTo(Embedding target){
        if(target instanceof EmbeddingTPGM) {
            byte[] idData = getIdData();
            byte[] propertyData = getPropertyData();
            byte[] idListData = getIdListData();

            byte[] newIdData = new byte[idData.length];
            byte[] newPropertyData = new byte[propertyData.length];
            byte[] newIdListData = new byte[idListData.length];
            ((EmbeddingTPGM) target).timeData = new byte[timeData.length];

            System.arraycopy(idData, 0, newIdData, 0, idData.length);
            System.arraycopy(propertyData, 0, newPropertyData, 0, propertyData.length);
            System.arraycopy(idListData, 0, newIdListData, 0, idListData.length);

            System.arraycopy(timeData, 0, ((EmbeddingTPGM) target).timeData, 0, timeData.length);
            target.setIdData(newIdData);
            target.setPropertyData(newPropertyData);
            target.setIdListData(newIdListData);
        }
        else{
            super.copyTo(target);
        }
    }

    @Override 
    public EmbeddingTPGM copy(){
        EmbeddingTPGM res = new EmbeddingTPGM();
        copyTo(res);
        return res;
    }

    @Override
    public void write(DataOutputView out) throws IOException{
        super.write(out);
        out.writeInt(timeData.length);
        out.write(timeData);
    }

    @Override
    public void read(DataInputView in) throws IOException{
        int sizeBuffer = in.readInt();
        byte[] ids = new byte[sizeBuffer];
        if (sizeBuffer > 0) {
            if (in.read(ids) != sizeBuffer) {
                throw new RuntimeException("Deserialisation of Embedding failed");
            }
        }

        sizeBuffer = in.readInt();
        byte[] newPropertyData =  new byte[sizeBuffer];
        if (sizeBuffer > 0) {
            if (in.read(newPropertyData) != sizeBuffer) {
                throw new RuntimeException("Deserialisation of Embedding failed");
            }
        }

        sizeBuffer = in.readInt();
        byte[] idLists = new byte[sizeBuffer];
        if (sizeBuffer > 0) {
            if (in.read(idLists) != sizeBuffer) {
                throw new RuntimeException("Deserialisation of Embedding failed");
            }
        }

        sizeBuffer = in.readInt();
        byte[] tData = new byte[sizeBuffer];
        if (sizeBuffer > 0) {
            if (in.read(tData) != sizeBuffer) {
                throw new RuntimeException("Deserialisation of Embedding failed");
            }
        }

        setIdData(ids);
        setPropertyData(newPropertyData);
        setIdListData(idLists);
        timeData = tData;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmbeddingTPGM that = (EmbeddingTPGM) o;

        if (!Arrays.equals(getIdData(), that.getIdData())) {
            return false;
        }
        if (!Arrays.equals(getPropertyData(), that.getPropertyData())) {
            return false;
        }
        if (!Arrays.equals(getIdListData(), that.getIdListData())){
            return false;
        }
        return Arrays.equals(timeData, that.timeData);
    }

    @Override
    public int hashCode(){
        int result = Arrays.hashCode(getIdData());
        result = 31 * result + Arrays.hashCode(getPropertyData());
        result = 31 * result + Arrays.hashCode(getIdListData());
        result = 31 * result + Arrays.hashCode(timeData);
        return result;
    }

    @Override
    public String toString(){
        List<List<GradoopId>> idCollection = new ArrayList<>();
        for (int i = 0; i < size(); i++) {
            idCollection.add(getIdAsList(i));
        }

        String idString = idCollection
                .stream()
                .map(entry -> {
                    if (entry.size() == 1) {
                        return entry.get(0).toString();
                    } else {
                        return entry.stream().map(GradoopId::toString).collect(joining(", ", "[", "]"));
                    }
                })
                .collect(joining(", "));

        String propertyString = getProperties()
                .stream()
                .map(PropertyValue::toString)
                .collect(joining(", "));

        Long[] t = byteArrayToLongs(timeData);
        StringBuilder tsb = new StringBuilder();
        for(int i=0; i<t.length; i+=4){
            tsb.append("(");
            tsb.append("tx_from: ").append(t[i]).append(", ");
            tsb.append("tx_to: ").append(t[i+1]).append(", ");
            tsb.append("val_from: ").append(t[i+2]).append(", ");
            tsb.append("val_to: ").append(t[i+3]);
            tsb.append(")");
        }

        return "Embedding{ " +
                "entries: {" + idString + "},  " +
                "properties: {" + propertyString + "}, " +
                "timedata: {" + new String(tsb) +"} "+
                "}";
    }

}

package scespet.core;

import gsa.esg.mekon.core.Function;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 28/03/2013
 * Time: 21:25
 * To change this template use File | Settings | File Templates.
 */
public abstract class CellMaintainer<K, V> implements Function {
    private VectorStream<K, V> sourceVector;
    private int index;
    private K key;

    protected CellMaintainer(VectorStream<K, V> sourceVector, int index, K key) {
        this.sourceVector = sourceVector;
        this.index = index;
        this.key = key;
    }

    @Override
    public boolean calculate() {
        V inputV = sourceVector.get(index);
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

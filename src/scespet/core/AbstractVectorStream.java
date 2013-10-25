package scespet.core;

import gsa.esg.mekon.core.EventGraphObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 21/11/2012
 * Time: 08:31
 * To change this template use File | Settings | File Templates.
 */
//todo: public abstract class AbstractVectorStream<K, F extends HasValue<V>, V> implements VectorStream<K, V> {
public abstract class AbstractVectorStream<K, V> implements VectorStream<K, V> {
    private Map<K, Integer> indicies = new HashMap<K, Integer>();
    private List<K> keys = new ArrayList<K>();
//    private List<F> functions = new ArrayList<F>();
    private List<HasValue<V>> valueHolders = new ArrayList<HasValue<V>>();

    public AbstractVectorStream() {
    }

    @Override
    public List<K> getKeys() {
        return keys;
    }

    @Override
    public K getKey(int i) {
        return keys.get(i);
    }

    public EventGraphObject getTrigger(int i) {
        return getValueHolder(i).getTrigger();
    }

    public HasValue<V> getValueHolder(int i) {
        if (i < 0) {
            throw new UnsupportedOperationException("Maybe vectors should allow a 'getOrCreate' for value holders to allow joins that become satisfied later?");
        }
        return valueHolders.get(i);
    }

    public abstract HasValue<V> newCell(int i, K key);

    @Override
    public int indexOf(K key) {
        Integer column = indicies.get(key);
        if (column == null) return -1;
        return column;
    }

    @Override
    public V get(int i) {
        return valueHolders.get(i).value();
    }

    public List<V> getValues() {
        // todo: replace with Guava
        ArrayList<V> valueSnap = new ArrayList<V>(size());
        for (int i=0; i<size(); i++) {
            V value = getValueHolder(i).value();
            valueSnap.add( value );
        }
        return valueSnap;
    }

    public int size() {
        return valueHolders.size();
    }

//    public F getAt(int i) {
//        return functions.get(i);
//    }
//
    public HasValue<V> get(K key) {
        Integer index = indicies.get(key);
        if (index == null) return null;
        return getValueHolder(index);
    }

    public int getIndex(K key) {
        Integer index = indicies.get(key);
        if (index == null) return -1;
        return index;
    }

    public int getSize() {
        return valueHolders.size();
    }

    public void add(K key) {
        Integer index = indicies.get(key);
        if (index == null) {
            index = keys.size();
            indicies.put(key, index);
            keys.add(key);

            // say this is not initialised, an implementation can override this to be true if necessary
            ReshapeSignal newColumnTrigger = getNewColumnTrigger();
            newColumnTrigger.newColumnAdded(index, false);
            HasValue<V> newValue = newCell(index, key);
            valueHolders.add(newValue);
//            F f = (F) newValue.getTrigger();
//            functions.add(f);
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("{");
        for (int i=0; i<getSize(); i++) {
            buf.append(getKey(i)).append("=").append(get(i)).append(", ");
        }
        if (getSize() > 0) {
            buf.delete(buf.length() - 2, buf.length());
        }
        buf.append("}");
        return buf.toString();
    }
}
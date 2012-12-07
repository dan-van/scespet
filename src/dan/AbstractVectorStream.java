package dan;

import stub.gsa.esg.mekon.core.EventGraphObject;
import stub.gsa.esg.mekon.core.Function;

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
public abstract class AbstractVectorStream<K, F extends Function, V> implements VectorStream<K, V> {
    private Map<K, Integer> indicies = new HashMap<K, Integer>();
    private List<K> keys = new ArrayList<K>();
    private List<F> functions = new ArrayList<F>();

    public AbstractVectorStream() {
    }

    @Override
    public List<K> geyKeys() {
        return keys;
    }

    @Override
    public K getKey(int i) {
        return keys.get(i);
    }

    public F getTrigger(int i) {
        return functions.get(i);
    }

    public abstract F newCell(int i, K key);

    public List<F> values() {
        return functions;
    }

    public int size() {
        return functions.size();
    }

    public F getAt(int i) {
        return functions.get(i);
    }

    public F get(K key) {
        Integer index = indicies.get(key);
        if (index == null) return null;
        return functions.get(index);
    }

    public int getIndex(K key) {
        Integer index = indicies.get(key);
        if (index == null) return -1;
        return index;
    }

    public int getSize() {
        return functions.size();
    }

    public void add(K key) {
        Integer index = indicies.get(key);
        if (index == null) {
            index = keys.size();
            indicies.put(key, index);
            keys.add(key);
            F newValue = newCell(index, key);
            functions.add(newValue);
        }
    }
}

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
public class DataVector {/* <K,V> implements VectorStream<K,V>{
    private Map<K, Integer> indicies = new HashMap<K, Integer>();
    private List<K> keys = new ArrayList<K>();
    private List<V> values = new ArrayList<V>();
    private ReshapeSignal reshapeSignal;

    public DataVector() {
        reshapeSignal = new ReshapeSignal();
    }

    public List<K> geyKeys() {
        return keys;
    }

    public V get(int i) {
        return values.get(i);
    }

    public K getKey(int i) {
        return keys.get(i);
    }

    @Override
    public ReshapeSignal getNewColumnTrigger() {
        return reshapeSignal;
    }

    public List<K> keys() {
        return keys;
    }

    public List<V> values() {
        return values;
    }

    public int size() {
        return values.size();
    }

    public V getAt(int i) {
        return values.get(i);
    }

    public V get(K key) {
        Integer index = indicies.get(key);
        if (index == null) return null;
        return values.get(index);
    }

    public int getIndex(K key) {
        Integer index = indicies.get(key);
        if (index == null) return -1;
        return index;
    }

    public int getSize() {
        return values.size();
    }

    public <KP extends K, VP extends V> void set(KP key, VP value) {
        Integer index = indicies.get(key);
        if (index == null) {
            index = keys.size();
            indicies.put(key, index);
            keys.add(key);
            values.add(value);
        } else {
            values.set(index, value);
        }
    }

    public void setAt(int i, V value) {
        values.set(i, value);
    }
    */
}

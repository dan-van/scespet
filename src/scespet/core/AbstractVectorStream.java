package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 21/11/2012
 * Time: 08:31
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractVectorStream<K, V> implements VectorStream<K, V> {
    private final static Logger logger = Logger.getLogger(AbstractVectorStream.class.getName());

    // TODO: stop worrying about external dependencies and use Trove native collections?
    private Map<K, Integer> indicies = new HashMap<K, Integer>();
    private List<K> keys = new ArrayList<K>();
    private List<Boolean> initialised = new ArrayList<Boolean>();
//    private List<F> functions = new ArrayList<F>();
    private List<HasValue<V>> valueHolders = new ArrayList<HasValue<V>>();
    private Environment env;

    public AbstractVectorStream(Environment env) {
        this.env = env;
    }

    @Override
    public List<K> getKeys() {
        return keys;
    }

    @Override
    public K getKey(int i) {
        return keys.get(i);
    }

    @Override
    public boolean initialised(int i) {
        return initialised.get(i);
    }

    public void setInitialised(int i) {
        initialised.set(i, true);
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

    public void add(final K key) {
        Integer index = indicies.get(key);
        if (index == null) {
            index = keys.size();
            indicies.put(key, index);
            keys.add(key);
            initialised.add(false);

            // say this is not initialised, an implementation can override this to be true if necessary
            ReshapeSignal newColumnTrigger = getNewColumnTrigger();
            newColumnTrigger.newColumnAdded(index, false);
            HasValue<V> newValue = newCell(index, key);
            valueHolders.add(newValue);
            if (!initialised(index)) {
                // strictly this is not safe, there is no causal ordering between this function that manages the 'initialised' state
                // and a downstream listener that may try to determine if the cell is initialised.
                // however I think that most cases would be using a trigger from the cell as reason-enough to assume there is a value
                // TODO: the correct fix to this would be to add 'initialised' to HasValue, which both solves the problem and
                // TODO: acknowledges that HasValue is conceptually a listenable Option
                final Integer finalIndex = index;
                final EventGraphObject cellFiredEvent = newValue.getTrigger();
                env.addListener(cellFiredEvent, new Function() {
                    public boolean calculate() {
                        logger.info("cell is now initialised for key "+key);
                        setInitialised(finalIndex);
                        env.removeListener(cellFiredEvent, this);
                        return false;
                    }
                });
            }

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

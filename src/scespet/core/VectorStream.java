package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.*;

/**
 * new keys are only ever added.
 */
public interface VectorStream<K, V> {
    int getSize();
    List<K> getKeys();
    List<V> getValues();

    // I don't think we'll need to call this often, only on join. I reckon an acceptable
    // implementation is just getKeys.indexOf(k)
    int indexOf(K key);
    V get(int i);
    K getKey(int i);

    /**
     * todo: how about adding initialised (or 'isDefined') to HasValue API?
     * return true if the given cell has ticked (or if it was built based on an input that had already ticked)
     */
    boolean initialised(int i);

    HasValue<V> getValueHolder(int i);

    // this should be redundant now
    EventGraphObject getTrigger(int i);

    ReshapeSignal getNewColumnTrigger();

    public static class ReshapeSignal implements Function {
        private Environment env;

        public ReshapeSignal(Environment env) {
            this.env = env;
        }

        private Map<Integer, Boolean> newColumnHasValue = Collections.emptyMap();
        private Map<Integer, Boolean> newColumnHasValue_pending = new TreeMap();
        @Override
        public boolean calculate() {
            newColumnHasValue = newColumnHasValue_pending;
            newColumnHasValue_pending = new TreeMap();
            return ! newColumnHasValue.isEmpty();
        }

        public void newColumnAdded(int i, boolean hasInitialValue) {
            if (newColumnHasValue_pending.isEmpty()) {
                env.wakeupThisCycle(this);
            }
            newColumnHasValue_pending.put(i, hasInitialValue);
        }

        public boolean newColumnHasValue(int i) {
            Boolean hasValue = newColumnHasValue.get(i);
            if (hasValue == null) throw new IllegalArgumentException(i+" is not a new column");
            return hasValue;
        }
    }
}

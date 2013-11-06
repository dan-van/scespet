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

    HasValue<V> getValueHolder(int i);

    // this should be redundant now
    EventGraphObject getTrigger(int i);

    ReshapeSignal getNewColumnTrigger();

    public static class ReshapeSignal implements Function {
        private Environment env;

        public ReshapeSignal(Environment env) {
            this.env = env;
        }

        // todo: remnant of an old implementation, clean up, maps no longer needed
        private Map<Integer, Boolean> newColumnHasValue = Collections.emptyMap();
        private Map<Integer, Boolean> newColumnHasValue_pending = new TreeMap();
        @Override
        public boolean calculate() {
            newColumnHasValue = newColumnHasValue_pending;
            newColumnHasValue_pending = new TreeMap();
            return ! newColumnHasValue.isEmpty();
        }

        public void newColumnAdded(int i) {
            if (newColumnHasValue_pending.isEmpty()) {
                env.wakeupThisCycle(this);
            }
            newColumnHasValue_pending.put(i, true);
        }
    }
}

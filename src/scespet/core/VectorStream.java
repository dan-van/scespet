package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.*;

/**
 * new keys are only ever added.
 */
public interface VectorStream<K, V> {
    /**
     * @return true if this vector is definitely empty, rather than some upstream source simply being in an unknown (not initialised) state.
     * I have not achieved inner peace on this one. The only current usecase is when applying
     * MultiStream.mapVector( mapFunc ) and it was plain that my mapFunc should not have been applied to an empty vector (because no events had been
     * consumed yet, that would have given rise to a non-empty vector).
     * Why didn't I just say that I should never apply mapVector to an empty vector?
     * - well, if we *had* seen an event, but filtered it and resulted in an empty vector, then that is a significant result and worthy of going into
     * the mapFunc.
     *
     * Anyway, wait for more usecase and thought on this one.
     */
    boolean isInitialised();
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

    /**
     * The thing that fires when some new columns have been added to this vector
     * @return
     */
    ReshapeSignal getNewColumnTrigger();

    public static class ReshapeSignal implements Function, EventGraphObject.Lifecycle {
        private Environment env;
        private VectorStream<?, ?> vector;      // this is mainly to aid debugging - which vector is this reshape relating too?

        public ReshapeSignal(Environment env, VectorStream<?,?> vector) {
            this.env = env;
            this.vector = vector;
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

//        @Override
//        public boolean init(Collection<EventGraphObject> initialisedInputs) {
//            return true;
//        }

        public void init() {
        }

        @Override
        public void destroy() {

        }

        public void newColumnAdded(int i) {
            if (newColumnHasValue_pending.isEmpty()) {
                if (!env.isFiring(this)) {
                    env.wakeupThisCycle(this);
                }
            }
            newColumnHasValue_pending.put(i, true);
        }

        @Override
        public String toString() {
            return "ReshapeTrigger{"+vector+"}";
        }
    }
}

package dan;

import stub.gsa.esg.mekon.core.EventGraphObject;
import stub.gsa.esg.mekon.core.Function;

import java.util.List;

/**
 * new keys are only ever added.
 */
public interface VectorStream<K, V> {
    int getSize();
    List<K> geyKeys();
    V get(int i);
    K getKey(int i);

    // todo: how about a HasVal<X> interface. i.e. a {trigger, value} tuple.
    EventGraphObject getTrigger(int i);
    ReshapeSignal getNewColumnTrigger();

    public static class ReshapeSignal implements Function {
        @Override
        public boolean calculate() {
            return true;
        }
    }
}
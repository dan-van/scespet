package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 29/11/2012
 * Time: 09:51
 * To change this template use File | Settings | File Templates.
 */

//TODO: public abstract class ChainedVector<K, F extends HasValue<V>, V> extends AbstractVectorStream<K, F, V> {
public abstract class ChainedVector<K, F extends EventGraphObject, V> extends AbstractVectorStream<K, F, V> {

    private final VectorStream.ReshapeSignal reshapeSignal;
    private final VectorStream<K, ?> sourceVector;
    private final Environment env;

    public ChainedVector(final VectorStream<K, ?> sourceVector, final Environment env) {
        this.sourceVector = sourceVector;
        this.env = env;

        // this listens to the source reshaping, applies our new columns, then fires on that we have reshaped
        reshapeSignal = new ReshapeSignal(env) {
            private int seenKeys = 0;
            private ReshapeSignal sourceVectorChanged = sourceVector.getNewColumnTrigger();
            {
                env.addListener(sourceVectorChanged, this);
            }
            public boolean calculate() {
                for (int i=seenKeys; i<sourceVector.getSize(); i++) {
                    K newKey = sourceVector.getKey(i);
                    add(newKey);
                }
                seenKeys = sourceVector.getSize();
                return super.calculate();
            }
        };
//        env.fireAfterChangingListeners()
        env.wakeupThisCycle(reshapeSignal);
//        reshapeSignal.calculate();
    }


    @Override
    public VectorStream.ReshapeSignal getNewColumnTrigger() {
        return reshapeSignal;
    }
}

package scespet.core;

import gsa.esg.mekon.core.Environment;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 29/11/2012
 * Time: 09:51
 * To change this template use File | Settings | File Templates.
 */

public abstract class ChainedVector<K, V> extends AbstractVectorStream<K, V> {

    private final VectorStream.ReshapeSignal reshapeSignal;
    private final VectorStream<K, ?> sourceVector;
    private final Environment env;
    private boolean initialised = false;

    public ChainedVector(final VectorStream<K, ?> sourceVector, final Environment env) {
        super(env);
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
                if (!initialised && sourceVector.isInitialised()) {
                    initialised = true;
                }
                for (int i=seenKeys; i<sourceVector.getSize(); i++) {
                    K newKey = sourceVector.getKey(i);
                    add(newKey);
                }
                seenKeys = sourceVector.getSize();
                return super.calculate();
            }
        };
        // we've just done some listener linkage, ripple an event after listeners established
        // not sure why we don't immediately do calculate here? Maybe because it would not propagate any mapping functions
        // but then again, surely the construction of a mapping function would use exactly the same 'should i init' approach?
        env.fireAfterChangingListeners(reshapeSignal);
//        env.wakeupThisCycle(reshapeSignal);
//        reshapeSignal.calculate();
    }

    @Override
    public boolean isInitialised() {
        return initialised;
    }

    protected VectorStream<K, ?> getSourceVector() {
        return sourceVector;
    }

    @Override
    public VectorStream.ReshapeSignal getNewColumnTrigger() {
        return reshapeSignal;
    }
}

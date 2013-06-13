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
public abstract class ChainedVector<K, F extends EventGraphObject, V> extends AbstractVectorStream<K, F, V> {

    private final VectorStream.ReshapeSignal reshapeSignal;
    private final VectorStream<K, ?> sourceVector;
    private final Environment env;

    public ChainedVector(final VectorStream<K, ?> sourceVector, final Environment env) {
        this.sourceVector = sourceVector;
        this.env = env;

        // this listens to the source reshaping, applies our new columns, then fires on that we have reshaped
        reshapeSignal = new ReshapeSignal(env) {
            private ReshapeSignal sourceVectorChanged = sourceVector.getNewColumnTrigger();
            {
                env.addListener(sourceVectorChanged, this);
            }
            public boolean calculate() {
                for (int i=getSize(); i<sourceVector.getSize(); i++) {
                    K newKey = sourceVector.getKey(i);
                    add(newKey);
                }
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

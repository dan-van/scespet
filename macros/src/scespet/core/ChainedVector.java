package scespet.core;

import scespet.core.AbstractVectorStream;
import scespet.core.VectorStream;
import stub.gsa.esg.mekon.core.Environment;
import stub.gsa.esg.mekon.core.Function;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 29/11/2012
 * Time: 09:51
 * To change this template use File | Settings | File Templates.
 */
public abstract class ChainedVector<K, F extends Function, V> extends AbstractVectorStream<K, F, V> {

    private final VectorStream.ReshapeSignal reshapeSignal;
    private final VectorStream<K, ?> sourceVector;
    private final Environment env;

    public ChainedVector(final VectorStream<K, ?> sourceVector, Environment env) {
        this.sourceVector = sourceVector;
        this.env = env;
        reshapeSignal = new ReshapeSignal() {
            public boolean calculate() {
                boolean added = false;
                for (int i=getSize(); i<sourceVector.getSize(); i++) {
                    K newKey = sourceVector.getKey(i);
                    add(newKey);
                    added = true;
                }
                return added;
            }
        };
        env.addListener(sourceVector.getNewColumnTrigger(), reshapeSignal);
//        env.fireAfterChangingListeners()
        env.wakeupThisCycle(reshapeSignal);
//        reshapeSignal.calculate();
    }

    @Override
    public abstract F newCell(int i, K key);

    @Override
    public VectorStream.ReshapeSignal getNewColumnTrigger() {
        return reshapeSignal;
    }
}

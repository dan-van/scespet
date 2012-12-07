package dan;

import stub.gsa.esg.mekon.core.Function;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 29/11/2012
 * Time: 09:51
 * To change this template use File | Settings | File Templates.
 */
public abstract class FunctionVector<K, F extends Function, V> extends AbstractVectorStream<K, F, V> {

    private final ReshapeSignal reshapeSignal;
    private VectorStream<K, ?> sourceVector;

    public FunctionVector() {
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
    }

    @Override
    public abstract F newCell(int i, K key);

    protected void setSource(VectorStream<K, ?> source) {
        this.sourceVector = source;
        // do the initial reshape (could do this as a fireAfterChangingListeners?)
        reshapeSignal.calculate();
    }

    @Override
    public ReshapeSignal getNewColumnTrigger() {
        return reshapeSignal;
    }
}

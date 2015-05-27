package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.*;
import java.util.logging.Logger;

/**
 * This represents an append-only set, but exposed as a VectorStream.
 * i.e. keys == values, however, if the values are actually EventGraphObjects then any chained vector will chain for cell updates.
 * e.g. you could have a MutableVector[TradeSource], then chain on a summation, to create a VectorStream[TradeSource, AccVol]
 *
 * it is very handy for taking a stream of values (e.g. stockSymbols), creating a VectorStream of the unique values, then using that to build
 * a vectorStream with symbol as the key, and something useful as the value.
 *
 * Think of this as a builder for the keyset of a map.
 *
 * @version $Id$
 * @author: danvan
 */
public class MutableVector<X> implements VectorStream<X,X> {
    private final static Logger logger = Logger.getLogger(MutableVector.class.getName());

    private Environment env;
    private ArrayList<X> values = new ArrayList<X>();
    private ArrayList<HasValue<X>> cells = new ArrayList<HasValue<X>>();
    private ArrayList<Boolean> initialised = new ArrayList<>();
    private Set<X> uniqueness = new HashSet<X>();

    final Function isInitialised = new Function() {
        public boolean calculate() {
            return false;
        }
    };

    final ReshapeSignal reshaped;

    public MutableVector(Iterable<X> initial, Environment env) {
        this(env);
        Iterator<X> iterator = initial.iterator();
        while (iterator.hasNext()) {
            X next = iterator.next();
            add(next);
        }
    }

    public MutableVector(Environment env) {
        this.env = env;
        reshaped = new ReshapeSignal(env, this);
        env.setStickyInGraph(reshaped, true);
    }

    @Override
    public boolean isInitialised() {
        return true;
    }

    public boolean add(X x) {
        if (uniqueness.add(x)) {
            int i = values.size();
            values.add(x);
            Cell<X> cell = new Cell<X>(x);
            cells.add(cell);
            reshaped.newColumnAdded(i);
            return true;
        }
        return false;
    }

    public boolean addAll(Iterable<X> xs) {
        boolean added = false;
        for (X x : xs) {
            // note, adding a colume will fire the reshaped signal
            added |= add(x);
        }
        return added;
    }

    public int getSize() {
        return values.size();
    }

    public List<X> getKeys() {
        return values;
    }

    public List<X> getValues() {
        return values;
    }

    public void setInitialised(int i) {
        initialised.set(i, true);
    }

    @Override
    public int indexOf(X key) {
        logger.warning("This could be slow");
        int idx = values.indexOf(key);
        if (idx < 0) {
            logger.warning(key+" not found");
        }
        return idx;
    }

    public X get(int i) {
        return values.get(i);
    }

    public X getKey(int i) {
        return values.get(i);
    }

    public EventGraphObject getTrigger(int i) {
        return cells.get(i).getTrigger();
    }

    public VectorStream.ReshapeSignal getNewColumnTrigger() {
        return reshaped;
    }

    @Override
    public HasValue<X> getValueHolder(int i) {
        return cells.get(i);
    }

    private class Cell<X> implements HasValue<X> {
        private final X value;
        private final EventGraphObject changeTrigger;

        public Cell(X value) {
            this.value = value;
            if (value instanceof EventGraphObject) {
                changeTrigger = (EventGraphObject) value;
            } else {
                Function oneShotInit = new Function() {
                    @Override
                    public boolean calculate() {
                        return true;
                    }
                };
                // this will ensure that anyone listening to this cell, or using env.isInitialised( me ) will behave correctly.
                env.wakeupThisCycle(oneShotInit);
                changeTrigger = oneShotInit;
            }
        }

        @Override
        public boolean initialised() {
            return true;
        }

        @Override
        public X value() {
            return value;
        }

        @Override
        public EventGraphObject getTrigger() {
            return changeTrigger;
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("ValueSet{");
        for (int i=0; i<getSize(); i++) {
            buf.append(getKey(i)).append(", ");
        }
        if (getSize() > 0) {
            buf.delete(buf.length() - 2, buf.length());
        }
        buf.append("}");
        return buf.toString();
    }

}

package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.*;

/**
 * @version $Id$
 * @author: danvan
 */
public class MutableVector<X> implements VectorStream<X,X> {
    private Class<X> type;
    private Environment env;
    private ArrayList<X> values = new ArrayList<X>();
    private Set<X> uniqueness = new HashSet<X>();
    boolean elementsListenable;

    final Function nullListenable = new Function() {
        public boolean calculate() {
            return false;
        }
    };

    final ReshapeSignal reshaped = new ReshapeSignal();

    public MutableVector(Class<X> type, Iterable<X> initial, Environment env) {
        this(type, env);
        Iterator<X> iterator = initial.iterator();
        while (iterator.hasNext()) {
            X next = iterator.next();
            if (uniqueness.add(next)) {
                values.add(next);
            }
        }
        // don't bother: env.wakeupThisCycle(reshaped);
    }

    public MutableVector(Class<X> type, Environment env) {
        this.type = type;
        this.env = env;
        elementsListenable = EventGraphObject.class.isAssignableFrom(type);
        env.setStickyInGraph(reshaped, true);
    }

    public boolean add(X x) {
        boolean added = uniqueness.add(x);
        if (added) {
            values.add(x);
            env.wakeupThisCycle(reshaped);
        }
        return added;
    }

    public boolean addAll(Iterable<X> xs) {
        boolean added = false;
        for (X x : xs) {
            boolean add = uniqueness.add(x);
            if (add) {
                added = true;
                values.add(x);
            }
        }
        if (added) {
            env.wakeupThisCycle(reshaped);
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

    public X get(int i) {
        return values.get(i);
    }

    public X getKey(int i) {
        return values.get(i);
    }

    public EventGraphObject getTrigger(int i) {
        if (elementsListenable) {
            return (EventGraphObject) values.get(i);
        } else {
            return nullListenable;
        }
    }

    public VectorStream.ReshapeSignal getNewColumnTrigger() {
        return reshaped;
    }
}

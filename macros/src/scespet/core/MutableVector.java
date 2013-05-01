package scespet.core;

import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @version $Id$
 * @author: danvan
 */
public class MutableVector<X> implements VectorStream<X,X> {
    private Class<X> type;
    private Environment env;
    ArrayList<X> values = new ArrayList<X>();
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
            values.add(next);
        }
        // don't bother: env.wakeupThisCycle(reshaped);
    }

    public MutableVector(Class<X> type, Environment env) {
        this.type = type;
        this.env = env;
        elementsListenable = EventGraphObject.class.isAssignableFrom(type);
        env.setStickyInGraph(reshaped, true);
    }

    public void add(X x) {
        values.add(x);
        env.wakeupThisCycle(reshaped);
    }

    public void addAll(Collection<X> xs) {
        values.addAll(xs);
        env.wakeupThisCycle(reshaped);
    }

    public int getSize() {
        return values.size();
    }

    public List<X> geyKeys() {
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

    public ReshapeSignal getNewColumnTrigger() {
        return reshaped;
    }
}

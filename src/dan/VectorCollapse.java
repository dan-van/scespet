package dan;

import scespet.core.VectorStream;
import gsa.esg.mekon.core.Environment;
import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 01/12/2012
 * Time: 21:59
 * To change this template use File | Settings | File Templates.
 */
public class VectorCollapse {
    private final VectorStream source;
    private final Function targetFunc;
    private final Environment env;

    public VectorCollapse(VectorStream source, Function collapseFunc, Environment env) {
        this.source = source;
        this.targetFunc = collapseFunc;
        this.env = env;

        RebindFunc rebindFunc = new RebindFunc();
        env.addListener(source.getNewColumnTrigger(), rebindFunc);
        env.addListener(rebindFunc, collapseFunc);
        if (source.getSize() > 0) {
            env.wakeupThisCycle(rebindFunc);
        }
    }

    private class RebindFunc implements Function {
        private int boundColumnCount = 0;
        @Override
        public boolean calculate() { // when we get new vector elements, just add a listener, and trigger a recalc
            int sourceSize = source.getSize();
            if (sourceSize > boundColumnCount) {
                for (int i = boundColumnCount; i < sourceSize; i++) {
                    EventGraphObject trigger =  source.getTrigger(i);
                    env.addListener(trigger, targetFunc);
                }
                boundColumnCount = sourceSize;
                // since new elements will be firing for their new values, I think maybe we shouldn't fire?
//                return true;
                return false;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "VectorCollapse-bindAll";
        }
    }
}

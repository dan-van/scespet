package gsa.esg.mekon.core;

import java.util.Collection;

/**
 * todo: could this be renamed as GraphListenable ?
 *
 * @author dvanenckevort_gsa
 */
public interface EventGraphObject {

    public interface Lifecycle extends EventGraphObject {
        /**
         * called when this node has been added to the graph
         * @param initialisedInputs the set of registered input dependencies that have had at least one init or calculate call applied
         */
        boolean init(Collection<EventGraphObject> initialisedInputs);

        /**
         * called when the node no longer present in the graph
         */
        void destroy();
    }
}

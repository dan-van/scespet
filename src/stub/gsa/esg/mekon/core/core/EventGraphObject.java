package stub.gsa.esg.mekon.core.core;

/**
 * todo: could this be renamed as GraphListenable ?
 *
 * @author dvanenckevort_gsa
 */
public interface EventGraphObject {

    public interface Lifecycle extends EventGraphObject {
        /**
         * called when this node has been added to the graph
         */
        void init();

        /**
         * called when the node no longer present in the graph
         */
        void destroy();
    }
}

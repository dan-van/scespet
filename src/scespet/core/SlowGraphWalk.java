package scespet.core;

import gsa.esg.mekon.core.EventGraphObject;
import gsa.esg.mekon.core.Function;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Logger;

/**
 * This is a very simplistic, inefficient implementation of a state propagation graph.
 * This is one implementation of the types.Env listener handling API, I have a significantly better implementation elsewhere.
 * However, it is enough to drive the unit tests and development of Scespet, even though in any real production system you'd want something
 * considerably better.
 *
 * Many concepts are not implemented in this class, or are only implemented to a bare minimum of correctness
 * the topology of the graph propagation is correct (but can't handle cycles)
 * 'wakeups' are poorly implemented.
 * the contract between graph structure modification and when events use that new structure is not completely robust.
 *
 *
 */
public class SlowGraphWalk {
    private static final Logger logger = Logger.getLogger(SlowGraphWalk.class.getName());

    EventTrace eventLogger = new EventTrace(this);
    private Node currentFiringNode = null;

    public class Node {
        private int lastFired = -1; // note that currentCycle is initialised to 0, ensuring that a newly built node never looks like it has fired.
        private int lastCalculated = -1;
        private Set<Node> in = new HashSet<Node>(2);
        private Set<Node> out = new HashSet<Node>(2);
        private Set<Node> in_orderingOnly = new HashSet<Node>(2); // used for non-triggering dependencies - as I've said this implementation is for testing rather than a nicely tuned efficient impl.
        private Set<Node> out_orderingOnly = new HashSet<Node>(2); // used for non-triggering dependencies - as I've said this implementation is for testing rather than a nicely tuned efficient impl.
        private EventGraphObject graphObject;
        private int order = -1;
        private int lastPropagationSweep = -1;

        private Node(EventGraphObject graphObject) {
            this.graphObject = graphObject;
            if (graphObject == null) throw new IllegalArgumentException("Null graphObject");
        }

        public EventGraphObject getGraphObject() {
            return graphObject;
        }

        public Collection<Function> getListeners() {
            ArrayList<Function> list = new ArrayList<Function>();
            for (Node node : out) {
                list.add((Function) node.getGraphObject());
            }
            return list;
        }

        public void addIn(Node sourceNode) {
            in.add(sourceNode);
        }

        public void removeIn(Node sourceNode) {
            in.remove(sourceNode);
        }

        public void addOut(Node targetNode) {
            out.add(targetNode);
        }

        public void addOut_Ordering(Node afterNode) {
            out_orderingOnly.add(afterNode);
        }
        public void addIn_Ordering(Node afterNode) {
            in_orderingOnly.add(afterNode);
        }

        public void removeOut(Node targetNode) {
            out.remove(targetNode);
        }

        public void trigger() {
            if (in.size() + in_orderingOnly.size() > 1) {
                // it is a rendezvous, punt it
                if (!joinNodes.contains(this)) {
                    joinNodes.add(this);
                }
            } else {
                // do it now.
                execute();
            }
        }

        private void execute() {
            currentFiringNode = this;
            boolean propagate = true;
            if (graphObject instanceof Function) {
                lastCalculated = cycleCount;
                propagate = ((Function) graphObject).calculate();
            }
            if (propagate) {
                lastFired = cycleCount;
                for (Node node : out) {
                    if (hasCalculated(node)) {
                        cyclicFires.add(node);
                        logger.info("Cyclic fire of " + node.graphObject);
                        // nudge my last-fired so that on the next cycle I show as having changed
                        lastFired = cycleCount + 1;
                    } else {
                        node.trigger();
                    }
                }
            }
        }
    }

    private static Comparator<Node> NODE_COMPARE = new Comparator<Node>() {
        @Override
        public int compare(Node o1, Node o2) {
            return o1.order - o2.order;
        }
    };
    private PriorityQueue<Node> joinNodes = new PriorityQueue<Node>(10, NODE_COMPARE);
    private Deque<Node> cyclicFires = new ArrayDeque<>(16);
    private IdentityHashMap<EventGraphObject, Node> nodes = new IdentityHashMap<EventGraphObject, Node>();
    private List<Node> newNodesThisCycle = new ArrayList<>();
    private int cycleCount = 0; // This counts event propagation to determine has-changed values, increments per cycle. Don't change init value without adjusting Node.lastFired init
    private int propagationSweep = 0; // to avoid recursion when propagating wiring changes in cyclic graphs
    private boolean isFiring = false;
    private boolean isChanging = false;
    // graph changes should occur between propagations to avoid very complex behaviour semantics
    private Deque<Runnable> deferredChanges = new ArrayDeque<Runnable>();
    private List<EventGraphObject> deferredFires = new ArrayList<>();

    public Collection<Node> getAllNodes() {
        Set<Node> allNodes = new HashSet<Node>();
        for (Node node : nodes.values()) {
            addToNodeSet(allNodes, node);
        }
        return allNodes;
    }

    // used to collect all connected incoming and outgoing nodes that connect to this one.
    private void addToNodeSet(Set<Node> allNodes, Node node) {
        if (allNodes.add(node)) {
            for (Node inNode : node.in) {
                addToNodeSet(allNodes, inNode);
            }
            for (Node outNode : node.out) {
                addToNodeSet(allNodes, outNode);
            }
        }
    }

    public void addTrigger(final EventGraphObject source, final Function target) {
        if (source == null) throw new IllegalArgumentException("Null source function firing "+target);
        if (target == null) throw new IllegalArgumentException("Null target function listening to "+source);
        Node sourceNode = getNode(source);
        Node targetNode = getNode(target);
        sourceNode.addOut(targetNode);
        targetNode.addIn(sourceNode);

        propagationSweep++;
        propagateOrder(targetNode, sourceNode.order);
        eventLogger.addListener(sourceNode, targetNode);

        if (hasCalculated(sourceNode) && !hasCalculated(targetNode)) {
            // Oops, this new link would not be activated, it missed the source firing. Catch up
            wakeup(target);
        }
    }

    public void removeTrigger(final EventGraphObject source, final Function target) {
        Node sourceNode = getNode(source);
        Node targetNode = getNode(target);
        if (currentFiringNode != sourceNode && hasCalculated(sourceNode) && hasChanged(sourceNode) && !hasCalculated(targetNode)) {
            throw new UnsupportedOperationException("NODEPLOY - I don't think I want to support this - removing listener edge before a fire can propagate, this would be rather non-deterministic I think");
        }
        // I think we should always apply remove after fire propagation. to dodge tricky questions of removing a link that has already fired (or maybe not fired)
        if (isFiring) {
            deferredChanges.add(new Runnable() {
                public void run() {
                    removeTrigger(source, target);
                }
            });
        } else {
            eventLogger.removeListener(sourceNode, targetNode);
            sourceNode.removeOut(targetNode);
            targetNode.removeIn(sourceNode);
        }
    }

    public void addWakeupDependency(final EventGraphObject source, final Function target) {
        Node sourceNode = getNode(source);
        Node targetNode = getNode(target);
        sourceNode.addOut_Ordering(targetNode);
        targetNode.addIn_Ordering(sourceNode);
        propagationSweep++;
        propagateOrder(targetNode, sourceNode.order);

    }

    public boolean hasChanged(EventGraphObject obj) {
        Node node = getNode(obj);
        return hasChanged(node);
    }

    private boolean hasChanged(Node node) {
        return node.lastFired >= cycleCount; // note that cycleCount can go backwards in the applyChanges initialisation block!
    }

    private boolean hasCalculated(Node node) {
        return node.lastCalculated >= cycleCount;
    }

    /**
     * true iff the object has ever had calculate or init called
     * @param obj
     * @return
     */
    public boolean isInitialised(EventGraphObject obj) {
        Node node = getNode(obj);
        return isInitialised(node);
    }

    private boolean isInitialised(Node node) {
        return node.lastFired >= 0;
    }

    public Iterable<EventGraphObject> getTriggers(EventGraphObject obj) {
        Node node = getNode(obj);
        // should I bring in Guava for functional filtered iterators, or just convert this to scala?
        ArrayList<EventGraphObject> changed = new ArrayList<>(node.in.size());
        for (Node in : node.in) {
            if (hasChanged(in)) {
                changed.add(in.getGraphObject());
            }
        }
        return changed;
    }

    private void propagateOrder(Node targetNode, int greaterThan) {
        if (targetNode.order <= greaterThan) {
            if (targetNode.lastPropagationSweep < propagationSweep) {
                targetNode.lastPropagationSweep = propagationSweep;
                int newGreater = greaterThan + 1;
                targetNode.order = newGreater;
                for (Node child : targetNode.out) {
                    propagateOrder(child, newGreater);
                }
                for (Node child : targetNode.out_orderingOnly) {
                    propagateOrder(child, newGreater);
                }
            } else {
                logger.info("Cyclic graph - I'm hoping my dumb implementation doesn't have to support this (though there are plenty of legitimate reasons to support this)");
            }
        }
    }

    private Node getNode(EventGraphObject source) {
        Node sourceNode = nodes.get(source);
        if (sourceNode == null) {
            sourceNode = new Node(source);
            nodes.put(source, sourceNode);
            newNodesThisCycle.add(sourceNode);
        }
        return sourceNode;
    }

    public void fire(EventGraphObject graphObject) {
        doFire(graphObject);

        // now process graph wiring
        applyChanges();
    }

    private void doFire(EventGraphObject graphObject) {
        isFiring = true;
        joinNodes.add(getNode(graphObject));
        do {
            cycleCount++;
            while (!joinNodes.isEmpty()) {
                Node next = joinNodes.remove();
                if (!hasCalculated(next)) {
                    eventLogger.beginWalk(next);
                    next.execute();
                } else { // a wakeup could have put this in here whilst a dependency could also want to fire it if we've been lazy and not registered all wakeup edges
                    // NODEPLOY is this a cycle?
                    logger.info("Looks like a non-registered wakeup?");
                }
            }
            if (cyclicFires.size() > 0) {
                joinNodes.addAll(cyclicFires);
                cyclicFires.clear();
            }
        } while (!joinNodes.isEmpty());
        isFiring = false;
    }

    public void applyChanges() {
        if (!isChanging) {
            while (!deferredChanges.isEmpty() || !deferredFires.isEmpty()) {
                if (!isChanging) {
                    eventLogger.beginApplyingChanges();
                    isChanging = true;
                }

                // each time round this loop is effectively a new cycle
                cycleCount += 1;
                for (Runnable deferredChange : deferredChanges) {
                    deferredChange.run();
                }
                deferredChanges.clear();

                // this is an interesting attempt to play with alternate approach to initialisation.
                // if a new node has never been initialised (i.e. had cacluate called), and it is listening to a source that has fired in the past
                // then tweak the 'hasChanged' logic and 'fire' the new node so that it can react to the input nodes.
                Collections.sort(newNodesThisCycle, new Comparator<Node>() {
                    @Override
                    public int compare(Node o1, Node o2) {
                        return o1.order - o2.order;
                    }
                });
                //NODEPLOY - no longer think that 'init' is necessary. Now that a 'wakeup this cycle' can be called from a constructor
                //NODEPLOY - and now that we can call 'isInitialised' for our sources, we should be able to do all the init we need without this.
                // NODEPLOY - TODO: track down whether we rely on this?
                int currentCycle = cycleCount;
                cycleCount = 0; // this effectively makes all nodes that have ever fired be hasChanged==true
                for (Node node : newNodesThisCycle) {
                    if (isInitialised(node)) {
                        logger.info("How is this node already initialised?: "+node);
                    } else {
                        EventGraphObject graphObject = node.getGraphObject();
                        List<EventGraphObject> initialisedInputs = new ArrayList<>(node.in.size());
                        for (Node inputNode : node.in) {
                            if (isInitialised(inputNode)) {
                                initialisedInputs.add(inputNode.getGraphObject());
                            }
                        }
                        if (!initialisedInputs.isEmpty()) {
                            boolean inited = false;
                            if (graphObject instanceof EventGraphObject.Lifecycle) {
                                inited = ((EventGraphObject.Lifecycle) graphObject).init(initialisedInputs);
//                            } else if (graphObject instanceof Function) {
//                                inited = ((Function) graphObject).calculate();
                            }
                            if (inited) {
                                node.lastFired = currentCycle;
                            }
                        }
                    }
                }
                // restore it
                cycleCount = currentCycle;
                newNodesThisCycle.clear();

                eventLogger.firingPostBuildEvents(deferredFires);
                for (EventGraphObject deferredFire : deferredFires) {
                    // NODEPLOY - I think this may now be unecessary!
                    Node node = getNode(deferredFire);
                    // this is in absence of using a set for deferred fires
                    if (node.lastFired < cycleCount) {
                        doFire(deferredFire);
                    }
                }
                deferredFires.clear();
            }
            if (isChanging) {
                isChanging = false;
                eventLogger.endApplyingChanges();
            }
        } else {
            throw new UnsupportedOperationException("Don't think I need to apply changes in nested fire");
        }
        // got deferred work to do, drain it:
        if (!joinNodes.isEmpty()) {
            Node node = joinNodes.poll();
            // this will drain the rest of the join nodes:
            fire(node.getGraphObject());
        }
    }

    public void wakeup(EventGraphObject graphObject) {
        if (currentFiringNode != null && currentFiringNode.graphObject == graphObject) return;   // redundant, and not bad. Already doing it.

        Node node = getNode(graphObject);
        if (hasCalculated(node)) {
            System.out.println("NODEPLOY - had your chance, muffed it: "+node);
        }
        if (!joinNodes.contains(node)) {
            joinNodes.add(node);
        }
    }

    public void fireAfterChangingListeners(EventGraphObject graphObject) {
        if (graphObject == null)
            throw new IllegalArgumentException("Can't fire a null object");
        deferredFires.add(graphObject);
    }

    public static class EventTrace {
        private Logger logger = Logger.getLogger("EventTrace");
        private SlowGraphWalk graph;
        private PrintStream out;

        public EventTrace(SlowGraphWalk graph) {
            this.graph = graph;
            try {
                Path outFile = Files.createTempFile("struct", "gdl");
                out = new PrintStream(outFile.toFile());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void beginWalk(Node entry) {
            logger.info("firing from: "+entry.getGraphObject());
        }

        public void addListener(Node source, Node target) {
            logger.info("addListener: "+source.getGraphObject()+" -> "+target.getGraphObject());
        }

        public void removeListener(Node source, Node target) {
            logger.info("removeListener: "+source.getGraphObject()+" -> "+target.getGraphObject());
        }

        public void restructured() {
            logger.info("Structure:");
            List<Node> all = new ArrayList<>(graph.getAllNodes());
            Collections.sort(all, new Comparator<Node>() {
                @Override
                public int compare(Node o1, Node o2) {
                    return o1.order - o2.order;
                }
            });
            for (Node node : all) {
                for (Function listener : node.getListeners()) {
                    logger.info("\t" + node.getGraphObject() + " -> " + listener);
                }
            }
        }

        public void beginApplyingChanges() {
            logger.info("Begin structure changes");
        }

        public void firingPostBuildEvents(List<EventGraphObject> deferredFires) {
            logger.info("Firing " + deferredFires.size() + " post-build events");
        }

        public void endApplyingChanges() {
            logger.info("End change block. New structure:");
            restructured();
        }
    }
}

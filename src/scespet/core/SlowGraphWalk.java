package scespet.core;

import stub.gsa.esg.mekon.core.EventGraphObject;
import stub.gsa.esg.mekon.core.Function;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 02/12/2012
 * Time: 22:19
 * To change this template use File | Settings | File Templates.
 */
public class SlowGraphWalk {

    private class Node {
        private int lastFired = -1;
        private Set<Node> in = new HashSet<Node>(2);
        private Set<Node> out = new HashSet<Node>(2);
        private EventGraphObject graphObject;
        private int order = -1;

        private Node(EventGraphObject graphObject) {
            this.graphObject = graphObject;
        }

        public void addIn(Node sourceNode) {
            in.add(sourceNode);
        }

        public void addOut(Node targetNode) {
            out.add(targetNode);
        }

        public void trigger() {
            if (in.size() > 1) {
                // it is a rendezvous, punt it
                joinNodes.add(this);
            } else {
                // do it now.
                execute();
            }
        }

        private void execute() {
            boolean propagate = true;
            if (graphObject instanceof Function) {
                if (lastFired == cycleCount) {
                    throw new AssertionError("cycle: "+cycleCount+" This simple graph walk is trying to double-fire a node: "+graphObject);
                } else {
                    lastFired = cycleCount;
                }
                propagate = ((Function) graphObject).calculate();
            }
            if (propagate) {
                for (Node node : out) {
                    node.trigger();
                }
            }
        }
    }

    private PriorityQueue<Node> joinNodes = new PriorityQueue<Node>(10, new Comparator<Node>() {
        @Override
        public int compare(Node o1, Node o2) {
            return o1.order - o2.order;
        }
    });
    private IdentityHashMap<EventGraphObject, Node> nodes = new IdentityHashMap<EventGraphObject, Node>();
    private int cycleCount = -1;

    public void addTrigger(EventGraphObject source, Function target) {
        Node sourceNode = getNode(source);
        Node targetNode = getNode(target);
        sourceNode.addOut(targetNode);
        targetNode.addIn(sourceNode);
        propagateOrder(targetNode, sourceNode.order);
    }

    private void propagateOrder(Node targetNode, int greaterThan) {
        if (targetNode.order <= greaterThan) {
            int newGreater = greaterThan + 1;
            targetNode.order = newGreater;
            for (Node child : targetNode.out) {
                propagateOrder(child, newGreater);
            }
        }
    }

    private Node getNode(EventGraphObject source) {
        Node sourceNode = nodes.get(source);
        if (sourceNode == null) {
            sourceNode = new Node(source);
            nodes.put(source, sourceNode);
        }
        return sourceNode;
    }

    public void fire(EventGraphObject graphObject) {
        cycleCount++;
        Node node = getNode(graphObject);
        joinNodes.add(node);
        while (!joinNodes.isEmpty()) {
            Node next = joinNodes.remove();
            next.execute();
        }
    }

    public void wakeup(EventGraphObject graphObject) {
        Node node = getNode(graphObject);
        if (!joinNodes.contains(node)) {
            joinNodes.add(node);
        }
    }
}

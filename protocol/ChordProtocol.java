package protocol;

import crypto.ConsistentHashing;
import p2p.NetworkInterface;
import p2p.NodeInterface;

import java.util.*;

/**
 * This class implements the chord protocol. The protocol is tested using the
 * custom built simulator.
 */
public class ChordProtocol implements Protocol {

    // length of the identifier that is used for consistent hashing
    public int m;

    // network object
    public NetworkInterface network;

    // consisent hasing object
    public ConsistentHashing ch;

    // key indexes. tuples of (<key name>, <key index>)
    public HashMap<String, Integer> keyIndexes;

    public ChordProtocol(int m) {
        this.m = m;
        setHashFunction();
        this.keyIndexes = new HashMap<String, Integer>();
    }

    /**
     * sets the hash function
     */
    public void setHashFunction() {
        this.ch = new ConsistentHashing(this.m);
    }

    /**
     * sets the network
     * 
     * @param network the network object
     */
    public void setNetwork(NetworkInterface network) {
        this.network = network;
    }

    /**
     * sets the key indexes. Those key indexes can be used to test the lookup
     * operation.
     * 
     * @param keyIndexes - indexes of keys
     */
    public void setKeys(HashMap<String, Integer> keyIndexes) {
        this.keyIndexes = keyIndexes;
    }

    /**
     *
     * @return the network object
     */
    public NetworkInterface getNetwork() {
        return this.network;
    }

    /**
     * This method builds the overlay network. It assumes the network object has
     * already been set. It generates indexes
     * for all the nodes in the network. Based on the indexes it constructs the ring
     * and places nodes on the ring.
     * algorithm:
     * 1) for each node:
     * 2) find neighbor based on consistent hash (neighbor should be next to the
     * current node in the ring)
     * 3) add neighbor to the peer (uses Peer.addNeighbor() method)
     */
    public void buildOverlayNetwork() {

        // Step 1: Get all nodes from the network and calculate their indexes
        // We'll use a TreeMap to automatically sort nodes by their index
        TreeMap<Integer, NodeInterface> sortedNodes = new TreeMap<>();

        // Get the topology (map of all nodes in the network)
        HashMap<String, NodeInterface> allNodes = network.getTopology();

        // Calculate hash index for each node and store in sorted map
        // Iterate through each node in the network
        for (Map.Entry<String, NodeInterface> entry : allNodes.entrySet()) {
            String nodeName = entry.getKey();
            NodeInterface node = entry.getValue();

            // Generate index using consistent hashing
            int nodeIndex = ch.hash(nodeName);

            // Set the node's ID to its hash index
            node.setId(nodeIndex);

            // Add to sorted map (TreeMap keeps entries sorted by key)
            sortedNodes.put(nodeIndex, node);
        }

        // Step 2: Build the ring by connecting each node to its successor
        // Convert the sorted nodes to a list for easier iteration
        List<NodeInterface> nodeList = new ArrayList<>(sortedNodes.values());

        // Connect each node to its successor (next node in the ring)
        int totalNodes = nodeList.size();

        for (int i = 0; i < totalNodes; i++) {
            NodeInterface currentNode = nodeList.get(i);

            // Find the successor (next node in circular order)
            // If we're at the last node, wrap around to the first node
            int nextPosition = i + 1;

            if (nextPosition >= totalNodes) {
                nextPosition = 0;
            }

            NodeInterface successorNode = nodeList.get(nextPosition);

            // Add the successor as a neighbor to the current node
            String successorName = successorNode.getName();
            currentNode.addNeighbor(successorName, successorNode);
        }
    }

    /**
     * This method builds the finger table for each node in the network.
     * Each finger table has m entries. Entry i contains:
     * - start: (n + 2^(i-1)) mod 2^m
     * - end: start of next entry - 1
     * - successor: first node responsible for the interval [start, end]
     */
    public void buildFingerTable() {

        // Get all nodes sorted by their index
        TreeMap<Integer, NodeInterface> sortedNodes = new TreeMap<>();
        HashMap<String, NodeInterface> allNodes = network.getTopology();

        for (Map.Entry<String, NodeInterface> entry : allNodes.entrySet()) {
            NodeInterface node = entry.getValue();
            sortedNodes.put(node.getId(), node);
        }

        List<NodeInterface> nodeList = new ArrayList<>(sortedNodes.values());

        // Build finger table for each node
        for (NodeInterface currentNode : nodeList) {

            int n = currentNode.getId(); // Current node's index
            int modulus = (int) Math.pow(2, m); // 2^m for wrap-around
            List<LinkedHashMap<String, Object>> fingerTable = new ArrayList<>();

            // Calculate m finger entries
            for (int i = 1; i <= m; i++) {

                // Calculate start: (n + 2^(i-1)) mod 2^m
                int start = (n + (int) Math.pow(2, i - 1)) % modulus;

                // Calculate end: (start of next entry - 1) mod 2^m
                int nextStart;
                if (i < m) {
                    nextStart = (n + (int) Math.pow(2, i)) % modulus;
                } else {
                    nextStart = (n + 1) % modulus; // Wraps to first entry
                }
                int end = (nextStart - 1 + modulus) % modulus;

                // Find successor: first node with index >= start
                NodeInterface successor = findSuccessor(start, nodeList, modulus);

                // Store entry: finger index, start, end, successor node
                LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
                entry.put("start", start);
                entry.put("end", end);
                entry.put("successor", successor);
                // only here to be able to read the value in print
                entry.put("successor Name", successor.getName());

                fingerTable.add(entry);
            }

            currentNode.setRoutingTable(fingerTable);
        }
    }

    /**
     * Helper method to find the successor node for a given start value
     * This finds the first node whose index >= start in the ring
     *
     * @param start    - the start value we're looking for
     * @param nodeList - sorted list of all nodes
     * @param modulus  - 2^m (for wrapping around)
     * @return the successor node
     */
    private NodeInterface findSuccessor(int start, List<NodeInterface> nodeList, int modulus) {

        // Go through all nodes in sorted order
        for (NodeInterface node : nodeList) {
            int nodeIndex = node.getId();

            // If this node's index >= start, it's the successor
            if (nodeIndex >= start) {
                return node;
            }
        }

        // If no node has index >= start, wrap around to the first node
        // (This handles the case where start is larger than all node indexes)
        return nodeList.get(0);
    }

    /**
     * This method performs the lookup operation.
     * Given the key index, it starts with one of the node in the network and
     * follows through the finger table.
     * The correct successors would be identified and the request would be checked
     * in their finger tables successively.
     * Finally the request will reach the node that contains the data item.
     *
     * @param keyIndex index of the key
     * @return names of nodes that have been searched and the final node that
     *         contains the key
     */
    public LookUpResponse lookUp(int keyIndex) {
        // Route tracking (to show which nodes were checked)
        LinkedHashSet<String> route = new LinkedHashSet<>();

        // Check if network is initialized
        if (network == null || network.getTopology().isEmpty()) {
            throw new IllegalStateException("Network is not initialized or empty");
        }

        // Choose the first available node as the starting node
        NodeInterface currentNode = network.getTopology().values().iterator().next();
        if (currentNode == null) {
            throw new IllegalStateException("Starting node not found in the network");
        }
        // Adds the checked node for tracking
        route.add(currentNode.getName());

        while (true) {
            // Check if the current node contains the key exactly
            if (keyIndex == currentNode.getId()){
                return new LookUpResponse(route, currentNode.getId(), currentNode.getName());
            }

            NodeInterface successorNode = currentNode.getSuccessor();
            // Checks if successorNode is null
            if (successorNode == null) {
                throw new IllegalStateException("Successor not found for node " + currentNode.getName());
            }
            int currentIndex = currentNode.getId();
            int successorIndex = successorNode.getId();

            // Check if the key lies between current node and its successor
            if (inInterval(keyIndex, currentIndex, successorIndex)){
                route.add(successorNode.getName());
                // Return the lookup result.
                return new LookUpResponse(route, successorNode.getId(), successorNode.getName());
            }

            // Otherwise, find the next node using finger table
            NodeInterface nextNode = null;

            List<LinkedHashMap<String, Object>> fingerTable = (List<LinkedHashMap<String, Object>>) currentNode.getRoutingTable();
            if  (fingerTable.size() == 0) {
                throw new IllegalStateException("Fingertable is not initialized");
            }

            /* Iterates backwards to prioritize larger intervals.
            *  This means it iterates from the last finger.
            *  Starting with the largerst interval allows lookup to make larger jumps towards the key,
            *  improving efficiency and aligning with Chords standard algorithm.
            *  This is because finger table entries covers exponensially increasing intervals.
            * */
            for (int i = fingerTable.size() - 1; i >= 0; i--) {
                LinkedHashMap<String, Object> entry = fingerTable.get(i);
                int start = (int) entry.get("start");
                NodeInterface fingerSuccessor = (NodeInterface) entry.get("successor");
                int successorId = fingerSuccessor.getId();
                //System.out.println("  Finger " + (i + 1) + ": start=" + start + ", successor=" + fingerSuccessor.getName() + ":" + successorId);
                if (inInterval(successorId, currentIndex, keyIndex)) {
                    nextNode = fingerSuccessor;
                    //System.out.println("  Key " + keyIndex + " closer to successor " + nextNode.getName() + ":" + successorId);
                    break;
                }
            }

            /* OLD LOOP TO ITERATE OVER FINGERTABLE
            // Iterates from the first finger (index 0) to the last.
            // Selects the first finger where keyIndex lies in the interval
            // [start, end]
            for (LinkedHashMap<String, Object> entry : fingerTable) {
                int start = (int) entry.get("start");
                int end = (int) entry.get("end");
                NodeInterface fingerSuccessor = (NodeInterface) entry.get("successor");
                if (inInterval(keyIndex, start, end)) {
                    nextNode = fingerSuccessor;
                    break;
                }
            }*/
            if (nextNode == null) {
                nextNode = successorNode;
                //System.out.println("No finger table match, falling back to successor: " + nextNode.getName() + ":" + nextNode.getId());
            }

            // Move to next node and continue
            currentNode = nextNode;
            route.add(currentNode.getName());

            // Safety check (to avoid infinite loops)
            if (route.size() > network.getTopology().size() + 2) {
                System.err.println("Lookup failed: possible ring misconfiguration.");
                break;
            }
        }
        return null; // if lookup fails
    }
    /* Helper function
    *  Checks if the keyIndex falls in a node's responsibility interval.
    *  This returns true if node contains the key.
    */
    private boolean inInterval(int key, int start, int end) {
        if (start <= end)
            return key >= start && key <= end;
        else // wrap-around
            return key >= start || key <= end;
    }
}
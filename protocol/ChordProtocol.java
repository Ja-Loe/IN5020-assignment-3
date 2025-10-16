package protocol;


import crypto.ConsistentHashing;
import p2p.NetworkInterface;
import p2p.NodeInterface;


import java.util.*;

/**
 * This class implements the chord protocol. The protocol is tested using the custom built simulator.
 */
public class ChordProtocol implements Protocol{

    // length of the identifier that is used for consistent hashing
    public int m;

    // network object
    public NetworkInterface network;

    // consisent hasing object
    public ConsistentHashing ch;

    // key indexes. tuples of (<key name>, <key index>)
    public HashMap<String, Integer> keyIndexes;


    public ChordProtocol(int m){
        this.m = m;
        setHashFunction();
        this.keyIndexes = new HashMap<String, Integer>();
    }



    /**
     * sets the hash function
     */
    public void setHashFunction(){
        this.ch = new ConsistentHashing(this.m);
    }



    /**
     * sets the network
     * @param network the network object
     */
    public void setNetwork(NetworkInterface network){
        this.network = network;
    }


    /**
     * sets the key indexes. Those key indexes can be used to  test the lookup operation.
     * @param keyIndexes - indexes of keys
     */
    public void setKeys(HashMap<String, Integer> keyIndexes){
        this.keyIndexes = keyIndexes;
    }



    /**
     *
     * @return the network object
     */
    public NetworkInterface getNetwork(){
        return this.network;
    }






    /**
     * This method builds the overlay network.  It assumes the network object has already been set. It generates indexes
     *     for all the nodes in the network. Based on the indexes it constructs the ring and places nodes on the ring.
     *         algorithm:
     *           1) for each node:
     *           2)     find neighbor based on consistent hash (neighbor should be next to the current node in the ring)
     *           3)     add neighbor to the peer (uses Peer.addNeighbor() method)
     */
    public void buildOverlayNetwork(){

        // Step 1: Get all nodes from the network and calculate their indexes
        // We'll use a TreeMap to automatically sort nodes by their index
        TreeMap<Integer, NodeInterface> sortedNodes = new TreeMap<>();

        // Get the topology (map of all nodes in the network)
        HashMap<String, NodeInterface> allNodes = network.getTopology();

        // Calculate hash index for each node and store in sorted map
        // Iterate through each node in the network
        for(Map.Entry<String, NodeInterface> entry: allNodes.entrySet()) {
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

        for(int i = 0; i < totalNodes; i++) {
            NodeInterface currentNode = nodeList.get(i);

            // Find the successor (next node in circular order)
            // If we're at the last node, wrap around to the first node
            int nextPosition = i + 1;

            if(nextPosition >= totalNodes) {
                nextPosition = 0;
            }

            NodeInterface successorNode = nodeList.get(nextPosition);

            // Add the successor as a neighbor to the current node
            String successorName = successorNode.getName();
            currentNode.addNeighbor(successorName, successorNode);
        }
    }






    /**
     * This method builds the finger table. The finger table is the routing table used in the chord protocol to perform
     * lookup operations. The finger table stores m-entries. Each ith entry points to the ith finger of the node.
     * Each ith entry stores the information of it's neighbor that is responsible for indexes ((n+2^i-1) mod 2^m).
     * i = 1,...,m.
     *
     *Each finger table entry should consists of
     *     1) start value - (n+2^i-1) mod 2^m. i = 1,...,m
     *     2) interval - [finger[i].start, finger[i+1].start)
     *     3) node - first node in the ring that is responsible for indexes in the interval
     */
    public void buildFingerTable() {
        /*
        implement this logic
         */

    }



    /**
     * This method performs the lookup operation.
     *  Given the key index, it starts with one of the node in the network and follows through the finger table.
     *  The correct successors would be identified and the request would be checked in their finger tables successively.
     *   Finally the request will reach the node that contains the data item.
     *
     * @param keyIndex index of the key
     * @return names of nodes that have been searched and the final node that contains the key
     */
    public LookUpResponse lookUp(int keyIndex){
        /*
        implement this logic
         */
        return null;
    }



}

/*
═══════════════════════════════════════════════════════════════════════════════════════════
                    SIMPLIFIED EXPLANATION OF buildOverlayNetwork()
═══════════════════════════════════════════════════════════════════════════════════════════

WHAT IS THIS FUNCTION DOING?
----------------------------
This function creates a "ring" of nodes. Think of it like arranging people in a circle where
everyone holds hands with the person next to them.

THE SIMPLE STEPS:
-----------------

1. COLLECT ALL NODES AND GIVE THEM NUMBERS
   - We get all the nodes from the network
   - Each node gets a special number (called an "index") by hashing its name
   - Example: "Node 1" might become index 5, "Node 2" becomes index 2, etc.
   - We store them in a TreeMap which automatically sorts them by their index

2. SORT THEM IN ORDER
   - The TreeMap automatically puts them in ascending order by index
   - Example: If we have indexes [5, 2, 3, 1], they get sorted to [1, 2, 3, 5]

3. CONNECT THEM IN A RING
   - We go through the sorted list
   - Each node gets connected to the NEXT node in the sorted order
   - The LAST node connects back to the FIRST node (making it a circle/ring)

   Example with 4 nodes:
   Node at index 1 → points to → Node at index 2
   Node at index 2 → points to → Node at index 3
   Node at index 3 → points to → Node at index 5
   Node at index 5 → points to → Node at index 1 (wraps around!)

WHAT DOES THIS CREATE?
-----------------------
A circular linked list (ring) where:
- Each node knows its "successor" (the next node clockwise in the ring)
- The nodes are arranged in numerical order by their hash index
- The last node connects back to the first node

WHY DO WE NEED THIS?
--------------------
- This ring structure is the foundation of the Chord protocol
- Later, keys (data) will be assigned to nodes based on this ring structure
- A key goes to the first node whose index is >= the key's index
- This makes it easy to find where data is stored

VISUAL EXAMPLE:
---------------
Before: Random nodes with names
After:  A ring like this:

        [0]
       ↗   ↘
    [7]     [1] Node4
     ↑       ↓
    [6]     [2] Node2
     ↑       ↓
    [5]     [3] Node3
   Node1     ↓
      ↖   [4] ↙

Each green node knows who comes next in the ring (its successor).

═══════════════════════════════════════════════════════════════════════════════════════════




═══════════════════════════════════════════════════════════════════════════════════════════
                    SIMPLIFIED EXPLANATION OF buildFingerTable()
═══════════════════════════════════════════════════════════════════════════════════════════

WHAT IS THIS FUNCTION DOING?
----------------------------
This function creates a "Finger Table" (routing table) for EACH node in the ring. Think of it
like giving each person a cheat sheet with shortcuts to reach other people in the circle
quickly, instead of going person-by-person around the whole circle.

WHY DO WE NEED THIS?
--------------------
Without finger tables, if Node 1 wants to find Node 100, it would have to go through:
Node 1 → Node 2 → Node 3 → ... → Node 99 → Node 100 (100 hops!)

With finger tables, Node 1 can jump to Node 50, then Node 75, then Node 90, then Node 100
(only 4 hops!). It's like having express lanes.

THE SIMPLE STEPS:
-----------------

For EACH node in the network:

1. CREATE A FINGER TABLE WITH 'm' ENTRIES
   - If m=3, each node gets 3 entries in its finger table
   - If m=10, each node gets 10 entries
   - Each entry is like a "shortcut" pointer to another node

2. FOR EACH ENTRY 'i' (from 1 to m), CALCULATE THREE THINGS:

   A) START VALUE
      - Formula: (nodeIndex + 2^(i-1)) mod 2^m
      - This tells us which range of keys this finger is responsible for
      - Example for Node 3 (m=3):
        * i=1: (3 + 2^0) mod 8 = (3 + 1) mod 8 = 4
        * i=2: (3 + 2^1) mod 8 = (3 + 2) mod 8 = 5
        * i=3: (3 + 2^2) mod 8 = (3 + 4) mod 8 = 7

   B) INTERVAL
      - This is the range [start, next_start)
      - Example for Node 3:
        * Entry 1: [4, 5)   → covers key 4
        * Entry 2: [5, 7)   → covers keys 5, 6
        * Entry 3: [7, 4)   → covers keys 7, 0, 1, 2, 3 (wraps around!)

   C) SUCCESSOR NODE
      - Find the FIRST node in the ring whose index is >= start value
      - This node is responsible for keys in that interval
      - Example for Node 3 (assuming nodes at indexes 1, 2, 3, 5):
        * Entry 1 (start=4): First node >= 4 is Node at index 5
        * Entry 2 (start=5): First node >= 5 is Node at index 5
        * Entry 3 (start=7): First node >= 7 is Node at index 1 (wraps around)

3. STORE THE FINGER TABLE IN THE NODE
   - Each node now has a complete finger table with all entries

CONCRETE EXAMPLE:
-----------------
Let's say we have 4 nodes with indexes [1, 2, 3, 5] and m=3

FINGER TABLE FOR NODE 3:
┌───────┬───────────┬─────────────────┐
│   i   │   start   │   successor     │
├───────┼───────────┼─────────────────┤
│   1   │     4     │   Node 1 (5)    │  ← Looking for index 4, found Node at 5
│   2   │     5     │   Node 1 (5)    │  ← Looking for index 5, found Node at 5
│   3   │     7     │   Node 4 (1)    │  ← Looking for index 7, wraps to Node at 1
└───────┴───────────┴─────────────────┘

FINGER TABLE FOR NODE 4 (index 1):
┌───────┬───────────┬─────────────────┐
│   i   │   start   │   successor     │
├───────┼───────────┼─────────────────┤
│   1   │     2     │   Node 2 (2)    │  ← Looking for index 2, found Node at 2
│   2   │     3     │   Node 3 (3)    │  ← Looking for index 3, found Node at 3
│   3   │     5     │   Node 1 (5)    │  ← Looking for index 5, found Node at 5
└───────┴───────────┴─────────────────┘

WHAT DOES THIS CREATE?
-----------------------
Each node now has:
- A finger table with 'm' entries
- Each entry points to a node that is roughly 2^(i-1) positions ahead in the ring
- The first entries point to nearby nodes (fine control)
- The later entries point to far away nodes (big jumps)

This creates a "logarithmic search" capability - you can find any node in O(log N) hops!

KEY CHARACTERISTICS:
--------------------
1. Each node stores information about only 'm' other nodes (not all N nodes!)
2. Nodes know MORE about nearby nodes (first few fingers)
3. Nodes know LESS about far away nodes (last few fingers)
4. The fingers create "exponentially increasing" jumps around the ring

HOW TO FIND THE SUCCESSOR NODE:
--------------------------------
To find "which node is responsible for index X":
1. Get all nodes in sorted order (from the ring we built earlier)
2. Find the first node whose index >= X
3. If no node has index >= X, wrap around to the first node (it's a ring!)

Example: Looking for index 4 with nodes [1, 2, 3, 5]
- Is 1 >= 4? No
- Is 2 >= 4? No
- Is 3 >= 4? No
- Is 5 >= 4? YES! Return Node at index 5

Example: Looking for index 7 with nodes [1, 2, 3, 5]
- Is 1 >= 7? No
- Is 2 >= 7? No
- Is 3 >= 7? No
- Is 5 >= 7? No
- Wrap around! Return first node (Node at index 1)

═══════════════════════════════════════════════════════════════════════════════════════════
*/

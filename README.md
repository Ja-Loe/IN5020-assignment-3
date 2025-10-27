IN5020-assignment-3 
# Chord Protocol Simulator

This project is a Java-based simulator for the Chord peer-to-peer (P2P) protocol. It simulates a network of nodes, builds the Chord ring overlay, constructs the finger tables for routing, and performs key lookups.

## Prerequisites

- **Java Development Kit (JDK) 21 or higher**: Required to compile and run the Java simulation. It could run on older versions, but was tested with JDK 21.

## Getting Started
### 1. Compile the Project
```bash
# Compile all .java files
javac Simulator.java ChordProtocolSimulator.java p2p/*.java protocol/*.java crypto/*.java

```
### 2. Run the Simulation

```bash
java Simulator <node_count> <m>
```
**Example Configurations**
These are the different configurations that are required:

```bash
# 10 Nodes, m=10
java Simulator 10 10
```

```bash
# 100 Nodes, m=20
java Simulator 100 20
```

```bash
# 1000 Nodes, m=20
java Simulator 1000 20
```
## Contributors

- Sigurd Smeby (sigursm@uio.no): TODO
- Håkon Rimer (haakori@uio.no): TODO
- Jakob Loe (jakobloe@uio.no): TODO
- Bahaa Aldeen Ghazal (bahaaalg@uio.no): `README.md` and testing.


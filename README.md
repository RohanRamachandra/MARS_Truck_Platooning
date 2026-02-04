MARS Truck Platooning

A simple Java simulation for truck platooning with merge-in-motion support.

Build

Run from the workspace root (requires Java 11+):

```powershell
javac -d bin src\*.java
```

Run

Start multiple nodes (one per terminal):

```powershell
java -cp bin TruckNode 1 8001
java -cp bin TruckNode 2 8002
java -cp bin TruckNode 3 8003
java -cp bin TruckNode 4 8004
```

Trigger a merge from a follower's console with:

```powershell
MERGE
```
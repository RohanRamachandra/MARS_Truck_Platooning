import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class TruckNode {
    final int nodeId;
    private final String nodeName;
    private final int port;
    private final List<Integer> peerPorts = Arrays.asList(8001, 8002, 8003, 8004); 
    private final TruckNodeEmergency emergencyHandler;

    
    // Use explicit map initialization for compatibility with Java 8 (Map.of is Java 9+)
    private static final Map<String, Integer> NAME_TO_ID;
    private static final Map<Integer, String> ID_TO_NAME;
    static {
        Map<String, Integer> m1 = new HashMap<>();
        m1.put("A", 1);
        m1.put("B", 2);
        m1.put("C", 3);
        m1.put("D", 4);
        NAME_TO_ID = Collections.unmodifiableMap(m1);

        Map<Integer, String> m2 = new HashMap<>();
        m2.put(1, "Alpha");
        m2.put(2, "Beta");
        m2.put(3, "Charlie");
        m2.put(4, "Delta");
        ID_TO_NAME = Collections.unmodifiableMap(m2);
    }

    volatile double vel = 20.0;
    volatile double desiredVel = 20.0;
    private volatile double gap = T_GAP;
    volatile double accel = 0.0;
    private volatile double lastAccel = 0.0;
    private volatile double desiredDistance = 5.0;
    private volatile double currentDistance = 50.0;
    private volatile int position = 1;
    private volatile int currentLeaderId = 1; 
    volatile boolean inPlatoon = false;
    private volatile boolean isActive = true;
    // Use CommManager to encapsulate communication-loss handling
    private final CommManager commManager;
    // Merge manager for join-in-motion operations
    private MergeManager mergeManager;

    // Reliability helpers for critical commands
    private final Map<Integer, AtomicLong> outgoingSeq = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> ackLatches = new ConcurrentHashMap<>();
    private final Set<String> ackReceived = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Clock matrix for causal ordering / knowledge tracking.
    // Matrix is sized (MAX_NODES+1) to allow direct 1-based indexing for node IDs.
    private static final int MAX_NODES = 4; // update if you add more nodes
    private final long[][] clockMatrix = new long[MAX_NODES + 1][MAX_NODES + 1];

    private static final double T_GAP = 0.6;
    private static final double D_MIN = 2.0;
    private static final double KP = 0.5;
    private static final double KD = 0.1;
    private static final double MAX_ACCEL = 2.0;
    private static final double MAX_JERK = 1.0;
    // Comm thresholds are handled by CommManager now

    private final Map<Integer, Long> onlineRegistry = new ConcurrentHashMap<>();
    private final List<Integer> platoonMembers = new CopyOnWriteArrayList<>();

    private volatile long lastElectionTime = 0;
	public  boolean emergencyActive;
    private static final long ELECTION_COOLDOWN = 500; // 500ms between elections for faster convergence

    // Merge tracking
    private volatile Integer activeMergingTruck = null;

    public TruckNode(int id, int port) {
        this.nodeId = id;
        this.port = port;
        this.emergencyHandler = new TruckNodeEmergency(this);
        this.mergeManager = new MergeManager(this);

        this.nodeName = ID_TO_NAME.getOrDefault(id, "Unknown");
        // Instantiate CommManager using optional system properties so tests can shorten thresholds
        long shortMs = Long.parseLong(System.getProperty("comm.shortMs", "5000"));
        long longMs = Long.parseLong(System.getProperty("comm.longMs", "10000"));
        double extraGap = Double.parseDouble(System.getProperty("comm.extraGap", "2.0"));
        // pass id->name map so logs show names
        Map<Integer, String> nameMap = new HashMap<>();
        nameMap.putAll(ID_TO_NAME);
        this.commManager = new CommManager(id, nameMap, shortMs, longMs, extraGap);

        // make sure outgoingSeq has an entry for this node if used
        outgoingSeq.putIfAbsent(id, new AtomicLong(0));

        if (id == 1) {
            this.inPlatoon = true;
            this.currentLeaderId = 1;
            platoonMembers.add(1);
        }

        // initialize clock matrix: zeroed; set own clock to 1 as a starting event
        synchronized (clockMatrix) {
            for (int i = 0; i <= MAX_NODES; i++) for (int j = 0; j <= MAX_NODES; j++) clockMatrix[i][j] = 0L;
            if (nodeId >= 1 && nodeId <= MAX_NODES) clockMatrix[nodeId][nodeId] = 1L;
        }
    }

    public void start() {
        printHeader();
        new Thread(this::receiveMessages).start();
        new Thread(this::commandConsole).start();

     // Start emergency handler threads
     // this.emergencyHandler.startControlLoop();
     // this.emergencyHandler.startUdpReceiver();
      //  this.emergencyHandler.startLocalSensor();

        
        // Need 3 threads: heartbeat sender, election task, and vehicle simulation/control
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(() -> {
            if (!isActive) {
                return;
            }
            if (nodeId == currentLeaderId) {
                cleanOfflineMembers();
                if (!platoonMembers.contains(nodeId)) {
                    platoonMembers.add(nodeId);
                }
                // leader heartbeat - let CommManager know leader HB just sent
                commManager.leaderHeartbeatReceived();
                sendToAll("HB|" + nodeId + "|" + currentLeaderId + "|" + getMemberIdsString());
            } else {
                sendToAll("HB|" + nodeId + "|" + currentLeaderId + "|" + nodeId);
            }
        }, 0, 1, TimeUnit.SECONDS);
        // dynamicLeaderElection should be suppressed while in DEGRADED mode; the CommManager will advise
        scheduler.scheduleAtFixedRate(() -> {
            // avoid leader election while degraded to prevent flapping
            if (commManager.isDegraded()) return;
            dynamicLeaderElection();
        }, 2, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(() -> {
            if (!isActive || !inPlatoon) return;
            
            if (emergencyHandler.getState() == State.EMERGENCY_BRAKE) {
                vel = 0.0;
                accel = -8.0;  // emergency deceleration
                return;        // skip normal simulation/control
            }
            
            // CommManager handles comm loss checks and metrics
            commManager.periodicCheck(() -> {
                // Trigger election only if this node is not currently the leader (follower detected prolonged loss)
                if (nodeId != currentLeaderId) {
                    System.out.println("[SYSTEM] CommManager triggered prolonged-loss callback on " + nodeName + ", initiating election...");
                    dynamicLeaderElection();
                }
            });
            simulateVehicleState();
            if (inPlatoon && nodeId != currentLeaderId) {
                updateControl(0.0);
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void printHeader() {
        System.out.println("=============================================");
        System.out.println("  MARS PLATOONING SYSTEM - NODE " + nodeName.toUpperCase());
        System.out.println("  NODE STATUS: ONLINE");
        System.out.println("=============================================");
    }

    // Public accessors used by tests
    public CommManager getCommManager() { return this.commManager; }
    public double getDesiredVel() { return this.desiredVel; }
    public double getGap() { return this.gap; }
    public String getNodeName() { return this.nodeName; }
    public int getTruckId() { return this.nodeId; }
    public int getCurrentLeaderId() { return this.currentLeaderId; }
    
    /**
     * Returns a MatrixClock wrapper for this node's clock matrix.
     * This is used for serialization and clock-based causal ordering.
     */
    public MatrixClock getMatrixClock() {
        return new MatrixClock(this.clockMatrix, this.nodeId);
    }

    // ===========================
    // MERGE-RELATED METHODS
    // ===========================
    
    private volatile double savedDesiredVel = 0.0;
    private volatile double savedGap = T_GAP;
    
    /**
     * Adjust vehicle speed for merge preparation.
     * Saves current state and adjusts to merge speed.
     */
    public void adjustSpeedForMerge() {
        this.savedDesiredVel = this.desiredVel;
        // Reduce speed slightly for safer merge
        this.desiredVel = Math.max(5.0, this.vel - 2.0);
        System.out.println("[MergeManager] Speed adjusted from " + String.format("%.2f", this.savedDesiredVel) 
            + " to " + String.format("%.2f", this.desiredVel) + " m/s");
    }
    
    /**
     * Restore vehicle speed after merge completion.
     */
    public void restoreSpeedAfterMerge() {
        this.desiredVel = this.savedDesiredVel;
        System.out.println("[MergeManager] Speed restored to " + String.format("%.2f", this.desiredVel) + " m/s");
    }
    
    /**
     * Restore gap spacing after merge completion.
     */
    public void restoreGapAfterMerge() {
        this.gap = this.savedGap;
        System.out.println("[MergeManager] Gap restored to " + String.format("%.2f", this.gap) + " seconds");
    }

    /**
     * Check if this node can initiate a merge.
     * Must be in platoon, not the leader, and not directly behind the leader.
     */
    public boolean canInitiateMerge() {
        if (!inPlatoon) return false;
        if (nodeId == currentLeaderId) return false;
        
        List<Integer> ordered = new ArrayList<>(platoonMembers);
        int myIndex = ordered.indexOf(nodeId);
        int leaderIndex = ordered.indexOf(currentLeaderId);

        // Must not be directly behind leader
        return myIndex > leaderIndex + 1;
    }

    /**
     * Check if leader can create a merge gap (feasibility).
     */
    public boolean canCreateMergeGap() {
        // Leader feasibility check
        return gap < 1.5;   // configurable threshold
    }

    /**
     * Create merge gap by increasing spacing.
     */
    public void createMergeGap() {
        System.out.println("[LEADER] Creating gap for merge");
        gap += 2.0;
    }

    /**
     * Check if merge gap is sufficient for insertion.
     */
    public boolean isMergeGapSufficient() {
        return gap >= 1.2;
    }

    /**
     * Increase gap for merge preparation (alternative name).
     */
    public void increaseGapForMerge() {
        this.gap = this.gap + 2.0;
        System.out.println("[MERGE] Gap increased to " + String.format("%.2f", this.gap) + " seconds");
    }

    /**
     * Get the merge manager instance.
     */
    public MergeManager getMergeManager() {
        return mergeManager;
    }

    /**
     * Broadcast platoon member update to all nodes.
     */
    private void broadcastPlatoonUpdate() {
           StringBuilder sb = new StringBuilder();
           for (int i = 0; i < platoonMembers.size(); i++) {
              if (i > 0) sb.append(",");
              sb.append(platoonMembers.get(i));
           }
           sendToAll("PLATOON_UPDATE|" + currentLeaderId + "|" + sb.toString());
    }

    /**
     * Check if this node is the leader.
     */
    public boolean isLeader() {
        return nodeId == currentLeaderId;
    }

        /**
         * Deduplicate and clean the platoon members list.
         */
        private void deduplicatePlatoonMembers() {
            if (platoonMembers.isEmpty()) return;
            List<Integer> seen = new ArrayList<>();
            List<Integer> duplicates = new ArrayList<>();
            for (int id : platoonMembers) {
                if (!seen.contains(id)) {
                    seen.add(id);
                } else {
                    duplicates.add(id);
                }
            }
            platoonMembers.removeAll(duplicates);
        }

    /**
     * Get current velocity for merge operations.
     */
    public double getVel() {
        return vel;
    }

    private void dynamicLeaderElection() {
        long now = System.currentTimeMillis();
        
        // Prevent election spam - only allow one election every 500ms
        if (now - lastElectionTime < ELECTION_COOLDOWN) {
            return;
        }
        lastElectionTime = now;

        // Always include self as a candidate; gather all nodes with recent activity
        List<Integer> candidates = new ArrayList<>();
        candidates.add(nodeId); // always include self
        
        // Add nodes with recent heartbeats (within commManager.getLongThresholdMs())
        for (Map.Entry<Integer, Long> e : onlineRegistry.entrySet()) {
            long last = e.getValue();
            if (now - last <= commManager.getLongThresholdMs()) {
                if (e.getKey() != nodeId) candidates.add(e.getKey());
            } else {
                onlineRegistry.remove(e.getKey());
            }
        }
        
        // Pick the minimum node ID as the new leader (deterministic by node ID)
        Collections.sort(candidates);
        int bestLeader = candidates.get(0);
        
        if (bestLeader != currentLeaderId) {
            int prior = currentLeaderId;
            currentLeaderId = bestLeader;
            inPlatoon = true;
            System.out.println("\n[SYSTEM] [" + nodeName + "] Leadership shift: " + ID_TO_NAME.get(bestLeader) + " is now in control. (previous: " + (prior==-1?"None":ID_TO_NAME.get(prior)) + ")\n         candidates=" + candidates + ", recent=" + onlineRegistry.keySet());
            
            if (nodeId == bestLeader) {
                // We are the new leader: initialize platoon with ourselves and recent nodes
                platoonMembers.clear();
                platoonMembers.add(nodeId);
                for (Integer id : onlineRegistry.keySet()) {
                    if (id != nodeId && !platoonMembers.contains(id)) platoonMembers.add(id);
                }
                // Inform CommManager we're the leader so it won't think we're lost
                commManager.leaderHeartbeatReceived();
                sendHeartbeatImmediately();
                System.out.println("[SYSTEM] [" + nodeName + "] Became leader. Platoon: " + getMemberNamesStringForLog());
            } else if (inPlatoon) {
                // We are a follower: request to join the new leader
                sendToSpecific(bestLeader, "JOIN_REQ|" + nodeId);
            }
        }
    }

    private void sendHeartbeatImmediately() {
        sendToAll("HB|" + nodeId + "|" + currentLeaderId + "|" + getMemberIdsString());
    }

    private void cleanOfflineMembers() {
        long now = System.currentTimeMillis();
        Set<Integer> offlineMembers = new HashSet<>();
        for (Integer memberId : new HashSet<>(platoonMembers)) {
            if (memberId == nodeId) continue;
            Long last = onlineRegistry.get(memberId);
            if (last == null || now - last > commManager.getLongThresholdMs()) {
                offlineMembers.add(memberId);
                onlineRegistry.remove(memberId);
            }
        }
        platoonMembers.removeAll(offlineMembers);
    }

    private double calculateDesiredDistance() {
        double effectiveGap = commManager.getEffectiveGap(gap);
        return effectiveGap * vel + D_MIN;
    }

    private double computePDControl(double error, double errorRate) {
        return KP * error + KD * errorRate;
    }

    private double boundAcceleration(double desiredAccel) {
        double jerkLimit = MAX_JERK * 0.1;
        double maxChange = lastAccel + jerkLimit;
        double minChange = lastAccel - jerkLimit;
        double bounded = Math.max(minChange, Math.min(maxChange, desiredAccel));
        bounded = Math.max(-MAX_ACCEL, Math.min(MAX_ACCEL, bounded));
        this.lastAccel = bounded;
        this.accel = bounded;
        return bounded;
    }

    private void updateControl(double predecessorAccel) {
        if (nodeId == currentLeaderId) return;
        
        this.desiredDistance = calculateDesiredDistance();
        double error = currentDistance - desiredDistance;
        double errorRate = (currentDistance - desiredDistance) * 0.1;
        double pdControl = computePDControl(error, errorRate);
        double feedforward = 0.5 * predecessorAccel;
        double desiredAccel = pdControl + feedforward;
        this.accel = boundAcceleration(desiredAccel);
        
        double maxVelChange = 0.5;
        if (Math.abs(this.vel - desiredVel) > maxVelChange) {
            this.vel += (desiredVel > this.vel ? maxVelChange : -maxVelChange);
        } else {
            this.vel = desiredVel;
        }
    }

    private void simulateVehicleState() {
        if (!inPlatoon || nodeId == currentLeaderId) return;
        
        Integer predecessorId = getPredecessorId();
        if (predecessorId == null) return;
        
        // We don't currently track remote velocities; as a best-effort estimate use desiredVel
        // (this keeps simulation moving rather than staying static)
        double predecessorVel = this.desiredVel;
        double relativeVel = predecessorVel - vel;
        double dt = 0.1;
        
        currentDistance += relativeVel * dt;
        if (currentDistance < D_MIN) currentDistance = D_MIN;
        if (currentDistance > 50.0) currentDistance = 50.0;
    }

    private Integer getPredecessorId() {
        List<Integer> sorted = new ArrayList<>(platoonMembers);
        Collections.sort(sorted);
        int idx = sorted.indexOf(nodeId);
        return idx > 0 ? sorted.get(idx - 1) : null;
    }

    private void updatePosition() {
        if (!inPlatoon) {
            this.position = -1;
            return;
        }
        List<Integer> sorted = new ArrayList<>(platoonMembers);
        Collections.sort(sorted);
        this.position = sorted.indexOf(nodeId) + 1;
    }

    private void commandConsole() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                try {
                    String input = scanner.nextLine().trim();
                    if (input.isEmpty()) continue;
                    String[] parts = input.split(" ", 3);
                    String cmd = parts[0].toUpperCase();

                    switch (cmd) {
                        case "J" -> {
                            if (!isActive) {
                                this.isActive = true;
                                System.out.println(">>> Rejoining platoon...");
                                dynamicLeaderElection();
                                System.out.println("[SYSTEM] Current leader: " + ID_TO_NAME.get(currentLeaderId));
                            } else if (inPlatoon && nodeId == currentLeaderId) {
                                System.out.println(">>> You are already the leader of the platoon.");
                            } else if (inPlatoon) {
                                System.out.println(">>> You are already in the platoon.");
                            } else {
                                sendToSpecific(currentLeaderId, "JOIN_REQ|" + nodeId);
                            }
                        }
                        case "L" -> {
                            sendToAll("LEAVE|" + nodeId);
                            this.inPlatoon = false;
                            this.isActive = false;
                            platoonMembers.remove(nodeId);
                            boolean wasLeader = (nodeId == currentLeaderId);
                            if (nodeId == currentLeaderId) {
                                this.currentLeaderId = -1;
                            }
                            System.out.println(">>> Leaving " + nodeName + "'s platoon as " + (wasLeader ? "leader." : "follower. Mode: Manual Override."));
                        }
                        case "P" -> {
                            String roleStatus;
                            if (!isActive) {
                                roleStatus = "MANUAL";
                            } else if (nodeId == currentLeaderId) {
                                roleStatus = "LEADER";
                            } else {
                                roleStatus = "FOLLOWER";
                            }
                            String leaderDisplay = currentLeaderId == -1 ? "None" : ID_TO_NAME.get(currentLeaderId);
                            System.out.println(">>> Role: " + roleStatus);
                            System.out.println(">>> Current Leader: " + leaderDisplay);
                            System.out.println(">>> Platoon Members: " + getMemberNamesStringForLog());
                        }
                        case "Q" -> {
                            updatePosition();
                            String role = !isActive ? "MANUAL" : (nodeId == currentLeaderId ? "LEADER" : "FOLLOWER");
                            String posStr = role.equals("LEADER") ? "Position 1 (LEADER)" : "Position " + position;
                            System.out.println(">>> Vehicle: " + nodeName);
                            System.out.println(">>> " + posStr);
                            System.out.println(">>> Role: " + role);
                        }
                        case "V" -> {
                            String roleV = !isActive ? "MANUAL" : (nodeId == currentLeaderId ? "LEADER" : "FOLLOWER");
                            this.desiredDistance = calculateDesiredDistance();
                            System.out.println(">>> Vehicle State: " + nodeName + " (" + roleV + ")");
                            System.out.println("    Velocity: " + String.format("%.2f", vel) + " m/s");
                            System.out.println("    Acceleration: " + String.format("%.2f", accel) + " m/s²");
                            System.out.println("    Current Distance: " + String.format("%.2f", currentDistance) + " m");
                            System.out.println("    Desired Distance: " + String.format("%.2f", desiredDistance) + " m");
                            System.out.println("    Gap: " + String.format("%.2f", gap) + " seconds" + (commManager.isInCommLoss() ? " (COMM LOSS - +2.0s)" : ""));
                        }
                        case "S" -> {
                            if (!isActive) {
                                System.out.println(">>> DENIED: Not in platoon");
                            } else if (nodeId != currentLeaderId) {
                                System.out.println(">>> DENIED: NOT AUTHORISED");
                            } else if (parts.length > 1) {
                                double v = Double.parseDouble(parts[1]);
                                this.vel = v;
                                for (Integer mid : platoonMembers) {
                                    if (mid != nodeId) sendToSpecific(mid, "CMD_SPEED|" + nodeId + "|" + v);
                                }
                                System.out.println(">>> Speed synced: " + String.format("%.2f", v) + " m/s");
                            }
                        }
                        case "G" -> {
                            if (!isActive) {
                                System.out.println(">>> DENIED: Not in platoon");
                            } else if (nodeId != currentLeaderId) {
                                System.out.println(">>> DENIED: NOT AUTHORISED");
                            } else if (parts.length > 1) {
                                double g = Double.parseDouble(parts[1]);
                                this.gap = g;
                                for (Integer mid : platoonMembers) {
                                    if (mid != nodeId) sendToSpecific(mid, "CMD_GAP|" + nodeId + "|" + g);
                                }
                                System.out.println(">>> Gap synced: " + String.format("%.2f", g) + " seconds");
                            }
                        }
                        case "M" -> {
                            if (parts.length >= 3) handleMessaging(parts);
                            else System.out.println(">>> Usage: M <TARGET> <MSG>");
                        }
                        case "LOSS" -> {
                            // Usage: LOSS 0.1  (set simulated packet loss fraction)
                            if (parts.length > 1) {
                                try {
                                    double r = Double.parseDouble(parts[1]);
                                    commManager.setLossRate(r);
                                    System.out.println(">>> CommManager lossRate set to " + r);
                                } catch (NumberFormatException ex) { System.out.println(">>> Invalid loss value"); }
                            } else {
                                System.out.println(">>> Usage: LOSS <rate> (0.0 - 1.0)");
                            }
                        }
                        case "CM" -> {
                            // Print comm manager metrics
                            System.out.println(">>> CommManager: state=" + commManager.getStateName() + ", lossRate=" + String.format("%.3f", commManager.getLossRate()));
                            System.out.println(">>> packets=" + commManager.getTotalPackets() + ", dropped=" + commManager.getDroppedPackets() + ", recentLoss=" + String.format("%.3f", commManager.getRecentLossRate()));
                            System.out.println(">>> totalDegradedMs=" + commManager.getTotalDegradedTimeMs());
                        }
                        case "C" -> {
                            // Display the local clock matrix
                            long[][] matrix = getClockMatrixCopy();
                            System.out.println(">>> Clock Matrix (node knowledge of logical clocks):");
                            System.out.print(">>>      ");
                            for (int j = 1; j <= MAX_NODES; j++) System.out.print("    N" + j);
                            System.out.println();
                            for (int i = 1; i <= MAX_NODES; i++) {
                                System.out.print(">>>  N" + i + " ");
                                for (int j = 1; j <= MAX_NODES; j++) System.out.print(String.format("%5d", matrix[i][j]));
                                System.out.println();
                            }
                        }
                        case "E" -> {
                            if (!isActive || !inPlatoon) {
                                System.out.println(">>> DENIED: Only platoon members can send emergency alerts.");
                            } else {
                                sendToAll("EMERGENCY|" + nodeId + "|STOP");
                                System.out.println(">>> Emergency sent to platoon!!!");
                               //Simulation sim = new Simulation(this);
                               // sim.triggerEmergency();
                                emergencyHandler.triggerLocalEmergency();
                                this.vel = 0;
                                this.accel = -8.0;
                                broadcastEmergency();
                            }
                        }
                        case "MERGE" -> {
                            if (!canInitiateMerge()) {
                                System.out.println(">>> MERGE DENIED: Not eligible");
                            } else {
                                mergeManager.initiateMerge();
                            }
                        }
                        default -> { }
                    }
                } catch (RuntimeException e) { System.out.println(">>> Command error. " + e.getMessage()); }
            }
        }
    }

    private void handleMessaging(String[] parts) {
        String target = parts[1].toUpperCase();
        String text = parts[2];
        if (target.equals("ALL")) {
            sendToAll("BROADCAST|" + nodeId + "|" + text);
            System.out.println(">>> Global broadcast sent to all: " + text);
        } else {
            int tId = NAME_TO_ID.getOrDefault(target, -1);
            if (onlineRegistry.containsKey(tId) || tId == nodeId) {
                sendToSpecific(tId, "MSG|" + nodeId + "|" + text);
                System.out.println(">>> Private message sent to " + target + ": " + text);
            } else {
                System.out.println(">>> ERROR: " + target + " is offline.");
            }
        }
    }

    private String getMemberIdsString() {
        StringBuilder sb = new StringBuilder();
        for (Integer id : platoonMembers) sb.append(id).append(",");
        return sb.toString();
    }

    // Return a comma-separated list of member names for human logs
    private String getMemberNamesStringForLog() {
        StringBuilder sb = new StringBuilder();
        List<String> names = new ArrayList<>();
        for (int id : platoonMembers) {
            String nm = ID_TO_NAME.get(id);
            if (nm != null) names.add(nm);
            else names.add("Unknown(" + id + ")");
        }
        Collections.sort(names);
        for (String n : names) {
            if (sb.length() > 0) sb.append(",");
            sb.append(n);
        }
        return sb.toString();
    }

    private void receiveMessages() {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            byte[] buffer = new byte[1024];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String raw = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    String msg = raw;
                    String matrixPart = null;
                    int nulIdx = raw.indexOf('\0');
                    if (nulIdx >= 0) {
                        msg = raw.substring(0, nulIdx);
                        matrixPart = raw.substring(nulIdx + 1);
                    }
                    // simulate experimental packet loss
                    if (commManager.shouldDropPacket()) {
                        // drop the packet (for experiments)
                        continue;
                    }
                    String[] p = msg.split("\\|", 4);
                    if (p.length < 2) continue;

                    int sId = Integer.parseInt(p[1]);
                    String sName = ID_TO_NAME.getOrDefault(sId, "???");
                    onlineRegistry.put(sId, System.currentTimeMillis());

                    switch (p[0]) {
                        case "ACK" -> {
                            // ACK|<seq>|<fromId>
                            try {
                                String seq = p.length > 1 ? p[1] : "";
                                String from = p.length > 2 ? p[2] : String.valueOf(sId);
                                String key = from + "-" + seq;
                                ackReceived.add(key);
                                CountDownLatch l = ackLatches.remove(key);
                                if (l != null) l.countDown();
                            } catch (RuntimeException ex) { System.err.println("ACK handling error: " + ex.getMessage()); }
                        }
                        case "HB" -> {
                            if (!isActive) {
                                if (p.length > 2) {
                                    int leaderId = Integer.parseInt(p[2]);
                                    if (leaderId != currentLeaderId) {
                                        this.currentLeaderId = leaderId;
                                    }
                                    if (p.length > 3) {
                                        platoonMembers.clear();
                                        String[] memberIds = p[3].trim().split(",");
                                        for (String idStr : memberIds) {
                                            String tid = idStr.trim();
                                            if (!tid.isEmpty()) {
                                                try {
                                                    int iid = Integer.valueOf(tid);
                                                    if (!platoonMembers.contains(iid)) platoonMembers.add(iid);
                                                } catch (NumberFormatException nfe) { }
                                            }
                                        }
                                    }
                                }
                            } else {
                                if (nodeId == currentLeaderId && p.length > 3) {
                                    if (inPlatoon && !platoonMembers.contains(sId)) platoonMembers.add(sId);
                                } else if (sId == currentLeaderId && p.length > 3 && nodeId != currentLeaderId) {
                                    commManager.leaderHeartbeatReceived();
                                    boolean wasInPlatoon = this.inPlatoon;
                                    platoonMembers.clear();
                                    String[] memberIds = p[3].trim().split(",");
                                    for (String idStr : memberIds) {
                                        String tid = idStr.trim();
                                        if (!tid.isEmpty()) {
                                            try {
                                                int iid = Integer.valueOf(tid);
                                                if (!platoonMembers.contains(iid)) platoonMembers.add(iid);
                                            } catch (NumberFormatException nfe) { }
                                        }
                                    }
                                    if (!wasInPlatoon) {
                                        System.out.println("\n[SYSTEM] Detected leader " + sName + ". Requesting to join...");
                                        sendToSpecific(sId, "JOIN_REQ|" + nodeId);
                                    } else {
                                        this.inPlatoon = true;
                                    }
                                }
                            }
                        }
                        case "JOIN_REQ" -> {
                            if (nodeId == currentLeaderId) {
                                if (!platoonMembers.contains(sId)) platoonMembers.add(sId);
                                sendToSpecific(sId, "JOIN_ACK|" + nodeId);
                                System.out.println("\n[PLATOON] Added " + sName);
                                sendToAll("JOIN_NOTIFY|" + sId + "|" + nodeId);
                            }
                        }
                        case "JOIN_ACK" -> {
                            this.inPlatoon = true;
                            this.currentLeaderId = sId;
                            platoonMembers.clear();
                            platoonMembers.add(sId);
                            platoonMembers.add(nodeId);
                            updatePosition();
                            System.out.println("\n[SUCCESS] Connected to " + sName + ". Platoon members now: " + getMemberNamesStringForLog());
                        }
                        case "JOIN_NOTIFY" -> {
                            try {
                                if (p.length > 2) {
                                    int leaderId = Integer.parseInt(p[2]);
                                    this.currentLeaderId = leaderId;
                                }
                                if (!platoonMembers.contains(sId)) platoonMembers.add(sId);
                            } catch (NumberFormatException ex) { /* ignore parse errors */ }
                            System.out.println("\n[NOTIFY] " + sName + " joined the platoon. Members: " + getMemberNamesStringForLog());
                        }
                        case "LEAVE" -> {
                            platoonMembers.remove(sId);
                            onlineRegistry.remove(sId);
                            System.out.println("\n[NOTIFY] " + sName + " left.");
                            if (sId == currentLeaderId) {
                                System.out.println("[SYSTEM] Leader " + sName + " disconnected. Electing new leader...");
                                dynamicLeaderElection();
                            }
                        }
                        case "CMD_SPEED" -> {
                            if (inPlatoon && p.length > 2) {
                                try {
                                    this.desiredVel = Double.parseDouble(p[2]);
                                    this.vel = this.desiredVel;
                                    System.out.println("\n[V2V] Speed synced: " + String.format("%.2f", vel) + " m/s");
                                } catch (NumberFormatException ex) { System.err.println("Invalid speed value: " + p[2]); }
                            }
                        }
                        case "CMD_GAP" -> {
                            if (inPlatoon && p.length > 2) {
                                try {
                                    this.gap = Double.parseDouble(p[2]);
                                    System.out.println("\n[V2V] Gap sync: " + String.format("%.2f", gap) + " seconds");
                                } catch (NumberFormatException ex) { System.err.println("Invalid gap value: " + p[2]); }
                            }
                        }
                        case "EMERGENCY" -> {
                        	 System.err.println("\n!!! BRAKING: Emergency Signal !!!");
                             this.vel = 0;
                             this.accel = -8.0;
                           
                        }
                        case "BROADCAST" -> {
                            if (p.length > 2) System.out.println("\n[MSG] " + sName + ": " + p[2]);
                        }
                        case "MSG" -> {
                            if (p.length > 2) System.out.println("\n[MSG] Private from " + sName + ": " + p[2]);
                        }
                        case "MERGE_JOIN_REQ" -> {
                            // Handle merge request from follower truck
                            if (p.length > 2) {
                                // Parse matrix clock if provided
                                try {
                                    int[][] receivedClock = MatrixClock.deserialize(p[2], MAX_NODES);
                                    getMatrixClock().update(sId, receivedClock);
                                    System.out.println("[CLOCK] Updated matrix clock on merge request from truck " + sName);
                                } catch (Exception ex) {
                                    System.err.println("[CLOCK] Clock parse error: " + ex.getMessage());
                                }
                            }
                            
                            if (nodeId == currentLeaderId) {
                                System.out.println("[LEADER] Merge request from Truck " + sName);
                                
                                // Prevent concurrent merges
                                if (activeMergingTruck != null && activeMergingTruck != sId) {
                                    System.out.println("[LEADER] Merge already in progress with truck " + activeMergingTruck + ", denying request");
                                    sendToSpecific(sId, "MERGE_ABORT|" + nodeId);
                                    break;
                                }
                                
                                activeMergingTruck = sId;
                                
                                // Remove old position if exists
                                platoonMembers.remove(Integer.valueOf(sId));
                                
                                // Insert directly after leader (position 2)
                                int leaderIndex = platoonMembers.indexOf(currentLeaderId);
                                platoonMembers.add(leaderIndex + 1, sId);
                                
                                System.out.println("[LEADER] New platoon order: " + getMemberNamesStringForLog());
                                
                                    // Clean up any duplicates before broadcasting
                                    deduplicatePlatoonMembers();
                                
                                // Broadcast updated platoon to all members
                                broadcastPlatoonUpdate();
                                
                                // Check if can create merge gap
                                if (canCreateMergeGap()) {
                                    System.out.println("[LEADER] Creating gap for merge");
                                    createMergeGap();
                                    sendToSpecific(sId, "MERGE_SLOT_ASSIGN|" + nodeId);
                                } else {
                                    System.out.println("[LEADER] Cannot create merge gap, denying merge");
                                    sendToSpecific(sId, "MERGE_ABORT|" + nodeId);
                                    activeMergingTruck = null;
                                }
                            }
                        }
                        case "MERGE_SLOT_ASSIGN" -> {
                            if (mergeManager != null) {
                                System.out.println("[" + nodeName + "] Slot assigned by leader");
                                sendToSpecific(currentLeaderId, "MERGE_PREPARE|" + nodeId);
                            }
                        }
                        case "MERGE_PREPARE" -> {
                            // MERGE_PREPARE is sent by the merging truck to the leader when it's ready.
                            if (nodeId == currentLeaderId) {
                                System.out.println("[LEADER] Received PREPARE from Truck " + sName);
                                // Commit the merge: instruct merging truck to commit and notify all
                                sendToSpecific(sId, "MERGE_COMMIT|" + nodeId);
                                sendToAll("MERGE_COMMIT|" + nodeId);
                                // broadcast final platoon order to ensure everyone is synchronized
                                broadcastPlatoonUpdate();
                                // mark merge complete on leader side
                                activeMergingTruck = null;
                            } else {
                                // non-leader (unlikely) let local MergeManager handle it if relevant
                                if (mergeManager != null) mergeManager.handleMergeMessage("MERGE_PREPARE");
                            }
                        }
                        case "MERGE_COMMIT" -> {
                            if (mergeManager != null) {
                                // Only followers should act on a MERGE_COMMIT; do not rebroadcast it.
                                if (nodeId == currentLeaderId) {
                                    // leader may receive its own commit or duplicates; ignore further propagation
                                    System.out.println("[LEADER] Received MERGE_COMMIT from " + sName + " (ignoring duplicate)");
                                } else {
                                    System.out.println("[MERGE] Committing merge for truck " + sName);
                                    mergeManager.handleMergeMessage("MERGE_COMMIT");
                                }
                                activeMergingTruck = null;
                            }
                        }
                        case "MERGE_ABORT" -> {
                            if (mergeManager != null) {
                                System.out.println("[" + nodeName + "] Merge aborted by leader");
                                mergeManager.handleMergeMessage("MERGE_ABORT");
                                if (nodeId == currentLeaderId) {
                                    activeMergingTruck = null;
                                }
                            }
                        }
                        case "PLATOON_UPDATE" -> {
                            // Update platoon member list from leader
                            if (p.length > 2) {
                                platoonMembers.clear();
                                    String[] ids = p[2].trim().split(",");
                                for (String id : ids) {
                                        String trimmedId = id.trim();
                                        if (!trimmedId.isEmpty()) {
                                        try {
                                            int iid = Integer.parseInt(trimmedId);
                                            if (!platoonMembers.contains(iid)) platoonMembers.add(iid);
                                        } catch (NumberFormatException ex) {
                                            // Skip invalid IDs
                                        }
                                    }
                                }
                                    // Final safety check for duplicates
                                    deduplicatePlatoonMembers();
                                System.out.println("[PLATOON] Order updated: " + getMemberNamesStringForLog());
                            }
                        }
                        default -> { }
                    }

                    // If a clock-matrix was attached, merge it into local matrix.
                    if (matrixPart != null) {
                        try {
                            mergeClockMatrixFromString(matrixPart);
                        } catch (RuntimeException ex) {
                            System.err.println("Clock matrix parse error: " + ex.getMessage());
                        }
                    }
                } catch (IOException ioe) {
                    System.err.println("IO error receiving packet: " + ioe.getMessage());
                } catch (NumberFormatException nfe) {
                    // bad incoming number, ignore that packet
                    System.err.println("Number format problem parsing incoming packet: " + nfe.getMessage());
                } catch (RuntimeException ex) {
                    System.err.println("Error processing incoming packet: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                }
            }
        } catch (SocketException se) {
            System.err.println("Receive socket error: " + se.getMessage());
        } catch (RuntimeException e) {
            System.err.println("General error in receiveMessages: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void sendToAll(String message) {
        for (int p : peerPorts) if (p != this.port) sendMessage(p, message);
    }

    public void sendToSpecific(int targetId, String message) {
        sendMessage(8000 + targetId, message);
    }

    private void sendMessage(int targetPort, String message) {
        try (DatagramSocket socket = new DatagramSocket()) {
            // Increment local clock for all sends (every outgoing message is an event)
            incrementLocalClock();
            byte[] buf = message.getBytes(StandardCharsets.UTF_8);
            // Optionally attach serialized clock matrix after a NUL separator when enabled
            if (Boolean.getBoolean("clock.matrix.enabled")) {
                String mat = serializeClockMatrix();
                byte[] mbytes = mat.getBytes(StandardCharsets.UTF_8);
                byte[] combined = new byte[buf.length + 1 + mbytes.length];
                System.arraycopy(buf, 0, combined, 0, buf.length);
                combined[buf.length] = 0;
                System.arraycopy(mbytes, 0, combined, buf.length + 1, mbytes.length);
                buf = combined;
            }
            socket.send(new DatagramPacket(buf, buf.length, InetAddress.getByName("localhost"), targetPort));
        } catch (IOException e) { System.err.println("Failed to send message to port " + targetPort + ": " + e.getMessage()); }
    }

    // Broadcast helpers for tests to reliably send critical commands to platoon members
    public void broadcastSpeed(double v) {
        this.desiredVel = v;
        for (Integer mid : platoonMembers) {
            if (mid != nodeId) {
                incrementLocalClock();
                sendReliableToSpecific(mid, "CMD_SPEED|" + nodeId + "|" + v, 3, 1000);
            }
        }
    }

    public void broadcastGap(double g) {
        this.gap = g;
        for (Integer mid : platoonMembers) {
            if (mid != nodeId) {
                incrementLocalClock();
                sendReliableToSpecific(mid, "CMD_GAP|" + nodeId + "|" + g, 3, 1000);
            }
        }
    }

    public void broadcastEmergency() {
        for (Integer mid : platoonMembers) {
            if (mid != nodeId) sendReliableToSpecific(mid, "EMERGENCY|" + nodeId + "|STOP", 3, 1000);
        }
    }

    // CLOCK MATRIX HELPERS
    private void incrementLocalClock() {
        synchronized (clockMatrix) {
            if (nodeId >= 1 && nodeId <= MAX_NODES) clockMatrix[nodeId][nodeId]++;
        }
    }

    private String serializeClockMatrix() {
        StringBuilder sb = new StringBuilder();
        synchronized (clockMatrix) {
            for (int i = 1; i <= MAX_NODES; i++) {
                for (int j = 1; j <= MAX_NODES; j++) {
                    if (j > 1) sb.append(',');
                    sb.append(clockMatrix[i][j]);
                }
                if (i < MAX_NODES) sb.append(';');
            }
        }
        return sb.toString();
    }

    private void mergeClockMatrixFromString(String s) {
        if (s == null || s.isEmpty()) return;
        String[] rows = s.split(";");
        long[][] incoming = new long[MAX_NODES + 1][MAX_NODES + 1];
        for (int i = 0; i < rows.length && i < MAX_NODES; i++) {
            String[] cols = rows[i].split(",");
            for (int j = 0; j < cols.length && j < MAX_NODES; j++) {
                incoming[i+1][j+1] = Long.parseLong(cols[j]);
            }
        }
        synchronized (clockMatrix) {
            for (int i = 1; i <= MAX_NODES; i++) {
                for (int j = 1; j <= MAX_NODES; j++) {
                    clockMatrix[i][j] = Math.max(clockMatrix[i][j], incoming[i][j]);
                }
            }
            // bump local clock to mark receive event
            if (nodeId >= 1 && nodeId <= MAX_NODES) clockMatrix[nodeId][nodeId]++;
        }
    }

    // Return a defensive copy for tests
    public long[][] getClockMatrixCopy() {
        long[][] copy = new long[MAX_NODES + 1][MAX_NODES + 1];
        synchronized (clockMatrix) {
            for (int i = 0; i <= MAX_NODES; i++) System.arraycopy(clockMatrix[i], 0, copy[i], 0, MAX_NODES + 1);
        }
        return copy;
    }

    /**
     * Send a critical message reliably to a specific node: attach a sequence number and wait for ACK with retries.
     */
    private boolean sendReliableToSpecific(int targetId, String message, int retries, long timeoutMs) {
        AtomicLong seqCounter = outgoingSeq.computeIfAbsent(targetId, k -> new AtomicLong(0));
        long seq = seqCounter.incrementAndGet();
        String key = targetId + "-" + seq;
        CountDownLatch latch = new CountDownLatch(1);
        ackLatches.put(key, latch);
        String full = "CRIT|" + seq + "|" + nodeId + "|" + message;
        boolean ok = false;
        for (int i = 0; i < retries; i++) {
            // increment own logical clock for this outgoing critical event
            incrementLocalClock();
            sendMessage(8000 + targetId, full);
            System.out.println("[RELIABLE] Sent seq=" + seq + " to " + targetId + " attempt=" + (i+1));
            try {
                if (latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    if (ackReceived.remove(key)) { ok = true; System.out.println("[RELIABLE] ACK received seq=" + seq + " from " + targetId); break; }
                } else {
                    System.out.println("[RELIABLE] No ACK for seq=" + seq + " from " + targetId + " on attempt=" + (i+1));
                }
            } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        ackLatches.remove(key);
        return ok;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java TruckNode <nodeId> <port>");
            System.err.println("Example: java TruckNode 1 8001");
            System.exit(1);
        }
        try {
            int nodeId = Integer.parseInt(args[0]);
            int port = Integer.parseInt(args[1]);
            new TruckNode(nodeId, port).start();
        } catch (NumberFormatException e) {
            System.err.println("Error: Node ID and port must be integers.");
            System.exit(1);
        }
    }
}
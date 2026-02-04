import java.util.Map;
import java.util.Random;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CommManager encapsulates communication-loss detection and handling.
 * Responsibilities:
 * - Simulate packet loss (for experiments)
 * - Track last leader heartbeat time and packet loss metrics
 * - Expose a simple state machine: NORMAL -> DEGRADED -> RECOVER
 * - Provide effective gap calculation and helpers for TruckNode
 * - Call a provided callback when prolonged loss is detected (optional)
 */
public class CommManager {
    public enum State { NORMAL, DEGRADED, RECOVER }

    // nodeId can be set after construction (TruckNode constructs early)
    private int nodeId;
    // optional mapping for pretty names (id -> name)
    private final Map<Integer, String> idToName;

    private final AtomicReference<State> state = new AtomicReference<>(State.NORMAL);
    private final AtomicLong lastLeaderHeartbeat = new AtomicLong(System.currentTimeMillis());

    // Loss simulation
    private volatile double lossRate = 0.0; // fraction [0.0..1.0]
    private final Random rng = new Random();

    // Metrics
    private final AtomicLong totalPackets = new AtomicLong(0);
    private final AtomicLong droppedPackets = new AtomicLong(0);
    private final AtomicLong degradedStartTime = new AtomicLong(0);
    private final AtomicLong totalDegradedTime = new AtomicLong(0);
    private final AtomicBoolean prolongedNotified = new AtomicBoolean(false);

    // thresholds (milliseconds)
    private final long shortThresholdMs;
    private final long longThresholdMs;
    private final double degradedExtraGapSeconds;
    // recovery hysteresis: require N consecutive good heartbeats to recover
    private final int recoveryHbNeeded;
    private final java.util.concurrent.atomic.AtomicInteger consecutiveGoodHbs = new java.util.concurrent.atomic.AtomicInteger(0);
    private volatile long lastGoodHbTime = 0;

    public CommManager(int nodeId) {
        this(nodeId, null, 5000L, 10000L, 2.0);
    }

    public CommManager(int nodeId, Map<Integer, String> idToName, long shortThresholdMs, long longThresholdMs, double extraGapSeconds) {
        this.nodeId = nodeId;
        this.idToName = idToName == null ? Collections.emptyMap() : new HashMap<>(idToName);
        this.shortThresholdMs = shortThresholdMs;
        this.longThresholdMs = longThresholdMs;
        this.degradedExtraGapSeconds = extraGapSeconds;
        // allow tuning via system property (useful for tests)
        int hbNeeded = 3;
        try { hbNeeded = Integer.parseInt(System.getProperty("comm.recoverHbs", "3")); } catch (Exception ex) { hbNeeded = 3; }
        this.recoveryHbNeeded = Math.max(1, hbNeeded);
    }

    // convenience constructor used earlier
    public CommManager(int nodeId, long shortThresholdMs, long longThresholdMs, double extraGapSeconds) {
        this(nodeId, null, shortThresholdMs, longThresholdMs, extraGapSeconds);
    }

    public void setLossRate(double rate) {
        if (rate < 0.0) rate = 0.0;
        if (rate > 1.0) rate = 1.0;
        this.lossRate = rate;
    }

    // Allow TruckNode to set the node id after constructing a placeholder
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    // Compatibility helper used by TruckNode
    public boolean isInCommLoss() { return isDegraded(); }

    public double getCommLossTimeGap() { return degradedExtraGapSeconds; }

    public double getLossRate() { return lossRate; }

    /**
     * Call when a leader heartbeat is received to reset timers and recover state.
     */
    public void leaderHeartbeatReceived() {
        long now = System.currentTimeMillis();
        lastLeaderHeartbeat.set(now);

        State prev = state.get();
        if (prev == State.DEGRADED) {
            // recovery hysteresis: require multiple consecutive good HB within shortThreshold window
            if (lastGoodHbTime > 0 && (now - lastGoodHbTime) <= shortThresholdMs) {
                int cnt = consecutiveGoodHbs.incrementAndGet();
                // only recover when we've seen enough consecutive good heartbeats
                if (cnt >= recoveryHbNeeded && state.compareAndSet(State.DEGRADED, State.NORMAL)) {
                    long start = degradedStartTime.getAndSet(0);
                    if (start > 0) totalDegradedTime.addAndGet(now - start);
                    System.out.println("\n[COMM] LOSS_RECOVERY: " + displayName(nodeId) + " recovered to NORMAL (" + cnt + " good HBs)");
                    // reset counters
                    consecutiveGoodHbs.set(0);
                    lastGoodHbTime = 0;
                    prolongedNotified.set(false);
                }
            } else {
                // first good heartbeat or gap too long - start counting
                consecutiveGoodHbs.set(1);
                lastGoodHbTime = now;
            }
        } else {
            // if we're already NORMAL, just reset counters
            consecutiveGoodHbs.set(0);
            lastGoodHbTime = now;
            prolongedNotified.set(false);
            state.set(State.NORMAL);
        }
    }

    /**
     * Periodic check that inspects lastLeaderHeartbeat and transitions the state machine.
     * If prolonged loss is detected it will call onProlongedLoss callback.
     */
    public void periodicCheck(Runnable onProlongedLoss) {
        long now = System.currentTimeMillis();
        long since = now - lastLeaderHeartbeat.get();

        State s = state.get();
        if (since > shortThresholdMs && s == State.NORMAL) {
            // Enter degraded
            if (state.compareAndSet(State.NORMAL, State.DEGRADED)) {
                degradedStartTime.compareAndSet(0, now);
                System.out.println("\n[COMM] LOSS_DETECT: " + displayName(nodeId) + " entering DEGRADED mode (no leader HB for " + since + " ms)");
            }
        }

        if (since > longThresholdMs && state.get() == State.DEGRADED) {
            // prolonged loss
            if (prolongedNotified.compareAndSet(false, true)) {
                System.out.println("\n[COMM] PROLONGED_LOSS: " + displayName(nodeId) + " has prolonged leader heartbeat absence (" + since + " ms)");
            }
            if (onProlongedLoss != null) {
                try { onProlongedLoss.run(); } catch (Exception ex) { System.err.println("Error in prolonged-loss callback: " + ex.getMessage()); }
            }
        }
    }

    /**
     * Simulate whether an incoming packet is dropped. Intended for experimentation.
     * Returns true when the packet should be dropped.
     */
    public boolean shouldDropPacket() {
        totalPackets.incrementAndGet();
        if (lossRate <= 0.0) return false;
        boolean drop = rng.nextDouble() < lossRate;
        if (drop) droppedPackets.incrementAndGet();
        return drop;
    }

    public boolean isDegraded() { return state.get() == State.DEGRADED; }

    /**
     * Effective gap to use given a base gap (seconds). In degraded mode the gap is increased.
     */
    public double getEffectiveGap(double baseGap) {
        return isDegraded() ? baseGap + degradedExtraGapSeconds : baseGap;
    }

    public long getTotalPackets() { return totalPackets.get(); }
    public long getDroppedPackets() { return droppedPackets.get(); }
    public double getRecentLossRate() {
        long t = totalPackets.get();
        if (t == 0) return 0.0;
        return (double) droppedPackets.get() / (double) t;
    }

    public long getTotalDegradedTimeMs() { return totalDegradedTime.get(); }

    public String getStateName() { return state.get().name(); }

    public long getLongThresholdMs() { return longThresholdMs; }

    private String displayName(int id) {
        if (idToName != null && idToName.containsKey(id)) return idToName.get(id) + " (" + id + ")";
        return "Node " + id;
    }
}
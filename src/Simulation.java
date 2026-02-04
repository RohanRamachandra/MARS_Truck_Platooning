import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/* ===================== SIMULATION CLASS ===================== */
public class Simulation {

    private final TruckNode truckNode;
    final TruckNodeEmergency emergencyHandler;

    public Simulation(TruckNode truckNode) {
        this.truckNode = truckNode;
        this.emergencyHandler = new TruckNodeEmergency(truckNode);
    }

    /**
     * Starts the simulation:
     * - emergency control loop
     * - UDP emergency receiver
     * - local sensor simulation
     */
    public void start() {
        System.out.println(truckNode.getNodeName() + ": 🚨 Simulation started (Emergency mode)");

        // Start the emergency handler threads
        emergencyHandler.startControlLoop();
        emergencyHandler.startUdpReceiver();
        emergencyHandler.startLocalSensor();
    }

    /**
     * Trigger an emergency manually (e.g., from E key press)
     * Broadcasts to followers and applies local braking
     */
    public void triggerEmergency() {
        System.out.println(truckNode.getNodeName() + ": ⚠️ Manual emergency triggered!");

        // Set emergency flag so TruckNode pauses normal velocity updates
        truckNode.emergencyActive = true;

        // Trigger emergency braking and broadcast
        emergencyHandler.triggerLocalEmergency();

        // Print matrix clock after triggering
        printMatrixClock();
    }

    /**
     * Print the current logical matrix clock for this truck
     */
    public void printMatrixClock() {
        long[][] matrix = emergencyHandler.getMatrixClock();
        System.out.println("\n[" + truckNode.getNodeName() + "] Current Matrix Clock:");
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                System.out.print(matrix[i][j] + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }
}


/* ===================== STATE ENUM ===================== */
enum State {
    PLATOON_FOLLOW,
    EMERGENCY_BRAKE,
    RECOVERY
}

/* ===================== MESSAGE CLASS ===================== */
class EmergencyMessage {
    String nodeId;
    double obstaclePosition;
    int severity;
    long timestamp;

    EmergencyMessage(String nodeId, double pos, int severity) {
        this.nodeId = nodeId;
        this.obstaclePosition = pos;
        this.severity = severity;
        this.timestamp = System.currentTimeMillis();
    }
}

/* ===================== TRUCK NODE EMERGENCY HANDLER ===================== */
class TruckNodeEmergency {
	
	
	private void printMatrixClock(String reason) {
	    System.out.println("\n🕒 MATRIX CLOCK @ " + truckNode.getNodeName() + " (" + reason + ")");
	    for (int i = 0; i < totalNodes; i++) {
	        for (int j = 0; j < totalNodes; j++) {
	            System.out.print(matrixClock[i][j] + " ");
	        }
	        System.out.println();
	    }
	    System.out.println("LogicalClock = " + logicalClock + "\n");
	}


    private static final int[] EMERGENCY_PORTS = {9001, 9002, 9003, 9004};

    private final int totalNodes = 4; // total number of trucks in platoon
    private long logicalClock = 0; // Lamport clock for this node
    private long[][] matrixClock = new long[totalNodes][totalNodes]; // matrix clock

    private final TruckNode truckNode;
    private final int emergencyPort;

    private volatile State state = State.PLATOON_FOLLOW;
    private final Lock controlLock = new ReentrantLock();
    private final Condition emergencySignal = controlLock.newCondition();

    private final BlockingQueue<EmergencyMessage> emergencyQueue =
            new PriorityBlockingQueue<>(10, (a, b) -> Long.compare(a.timestamp, b.timestamp));

    private volatile boolean running = true;

    public TruckNodeEmergency(TruckNode truckNode) {
        this.truckNode = truckNode;
        this.emergencyPort = 9000 + truckNode.nodeId;
        matrixClock[truckNode.nodeId - 1][truckNode.nodeId - 1] = logicalClock;
    }

    public long[][] getMatrixClock() {
    	 return copyMatrix(matrixClock);
	}

	/* ================= CONTROL LOOP ================= */
    public void startControlLoop() {
        new Thread(() -> {
            while (running) {
                controlLock.lock();
                try {
                    if (state == State.PLATOON_FOLLOW) {
                        emergencySignal.await(); // wait for emergency
                    }

                    if (state == State.EMERGENCY_BRAKE) {
                        applyEmergencyBraking();
                        state = State.RECOVERY;
                        recoverFromEmergency();
                    }

                } catch (InterruptedException ignored) {
                } finally {
                    controlLock.unlock();
                }
            }
        }, "CONTROL-" + truckNode.getNodeName()).start();
    }

    /* ================= MANUAL TRIGGER ================= */
    public void triggerLocalEmergency() {
        controlLock.lock();
        try {
            state = State.EMERGENCY_BRAKE;
            emergencySignal.signal();
        } finally {
            controlLock.unlock();
        }

        // Apply local braking
        truckNode.vel = 0;
        truckNode.accel = -8.0;

        // Update matrix clock
        logicalClock++;
        matrixClock[truckNode.nodeId - 1][truckNode.nodeId - 1] = logicalClock;
        
        printMatrixClock("LOCAL EMERGENCY TRIGGERED");


        // Broadcast emergency with matrix clock
        broadcastEmergency(new EmergencyMessage(truckNode.getNodeName(), 0.0, 5, copyMatrix(matrixClock)));
    }

    private void applyEmergencyBraking() {
        System.out.println(truckNode.getNodeName() + ": 🚨 EMERGENCY BRAKE APPLIED!");
        truckNode.accel = -8.0;
        truckNode.vel = 0.0;
        truckNode.inPlatoon = true;

        // Broadcast emergency to other trucks with matrix clock
        broadcastEmergency(new EmergencyMessage(truckNode.getNodeName(), 0.0, 5, copyMatrix(matrixClock)));
    }

    private void recoverFromEmergency() {
        new Thread(() -> {
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            truckNode.vel = truckNode.desiredVel;
            truckNode.accel = 0.0;
            state = State.PLATOON_FOLLOW;

            // Reset TruckNode emergency flag so followers resume normal control
            truckNode.emergencyActive = false;

            System.out.println(truckNode.getNodeName() + ": 🚦 Resuming normal platoon operation");
        }).start();
    }

    /* ================= UDP RECEIVER ================= */
    public void startUdpReceiver() {
        new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(emergencyPort)) {
                System.out.println(truckNode.getNodeName() +
                        ": Listening for emergency on port " + emergencyPort);

                byte[] buffer = new byte[4096];

                while (running) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    EmergencyMessage msg = deserialize(packet.getData());
                    handleEmergency(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "COMM_RECV-" + truckNode.getNodeName()).start();
    }

    private void handleEmergency(EmergencyMessage msg) {
        // merge matrix clocks
        for (int i = 0; i < totalNodes; i++) {
            for (int j = 0; j < totalNodes; j++) {
                matrixClock[i][j] = Math.max(matrixClock[i][j], msg.matrix[i][j]);
            }
        }

        // increment own logical clock
        logicalClock++;
        matrixClock[truckNode.nodeId - 1][truckNode.nodeId - 1] = logicalClock;

        emergencyQueue.offer(msg);

        controlLock.lock();
        try {
            state = State.EMERGENCY_BRAKE;
            emergencySignal.signal();
        } finally {
            controlLock.unlock();
        }

        truckNode.vel = 0;
        truckNode.accel = -8.0;
        truckNode.emergencyActive = true;
        
        printMatrixClock("EMERGENCY RECEIVED & MERGED");

    }

    /* ================= LOCAL SENSOR ================= */
    public void startLocalSensor() {
        new Thread(() -> {
            try {
                Thread.sleep(3000); // simulate detection delay
                detectObstacleLocally(120.0, 5);
            } catch (InterruptedException ignored) {}
        }, "SENSOR-" + truckNode.getNodeName()).start();
    }

    private void detectObstacleLocally(double pos, int severity) {
        System.out.println(truckNode.getNodeName() + ": ⚠️ LOCAL obstacle detected!");

        logicalClock++;
        matrixClock[truckNode.nodeId - 1][truckNode.nodeId - 1] = logicalClock;

        broadcastEmergency(new EmergencyMessage(truckNode.getNodeName(), pos, severity, copyMatrix(matrixClock)));

        controlLock.lock();
        try {
            state = State.EMERGENCY_BRAKE;
            emergencySignal.signal();
        } finally {
            controlLock.unlock();
        }

        truckNode.vel = 0;
        truckNode.accel = -8.0;
        truckNode.emergencyActive = true;
    }

    /* ================= UDP BROADCAST ================= */
    public void broadcastEmergency(EmergencyMessage msg) {
        logicalClock++;
        matrixClock[truckNode.nodeId - 1][truckNode.nodeId - 1] = logicalClock;

        // attach matrix to message
        StringBuilder matrixData = new StringBuilder();
        for (int i = 0; i < totalNodes; i++) {
            for (int j = 0; j < totalNodes; j++) {
                matrixData.append(matrixClock[i][j]).append(";");
            }
        }

        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] data = (serialize(msg) + "|" + matrixData).getBytes();

            for (int port : EMERGENCY_PORTS) {
                if (port == emergencyPort) continue;

                DatagramPacket packet = new DatagramPacket(
                        data,
                        data.length,
                        InetAddress.getByName("localhost"),
                        port
                );

                socket.send(packet);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        printMatrixClock("BROADCAST SENT");

    }

    /* ================= SERIALIZATION ================= */
    private byte[] serialize(EmergencyMessage msg) {
        return (msg.nodeId + "," +
                msg.obstaclePosition + "," +
                msg.severity + "," +
                msg.timestamp).getBytes();
    }

    private EmergencyMessage deserialize(byte[] data) {
        String[] parts = new String(data).trim().split("\\|");
        String[] vals = parts[0].split(",");
        EmergencyMessage msg = new EmergencyMessage(
                vals[0],
                Double.parseDouble(vals[1]),
                Integer.parseInt(vals[2]),
                new long[totalNodes][totalNodes]
        );
        msg.timestamp = Long.parseLong(vals[3]);

        if (parts.length > 1) {
            String[] matrixVals = parts[1].split(";");
            int idx = 0;
            for (int i = 0; i < totalNodes; i++)
                for (int j = 0; j < totalNodes; j++)
                    msg.matrix[i][j] = Long.parseLong(matrixVals[idx++]);
        }

        return msg;
    }

    /* ================= MATRIX CLOCK HELPER ================= */
    private long[][] copyMatrix(long[][] src) {
        long[][] copy = new long[totalNodes][totalNodes];
        for (int i = 0; i < totalNodes; i++)
            System.arraycopy(src[i], 0, copy[i], 0, totalNodes);
        return copy;
    }

    /* ================= MESSAGE CLASS WITH MATRIX ================= */
    static class EmergencyMessage {
        String nodeId;
        double obstaclePosition;
        int severity;
        long timestamp;
        long[][] matrix;

        EmergencyMessage(String nodeId, double pos, int severity, long[][] matrix) {
            this.nodeId = nodeId;
            this.obstaclePosition = pos;
            this.severity = severity;
            this.timestamp = System.currentTimeMillis();
            this.matrix = matrix;
        }
    }

    public State getState() {
        return state;
    }
}

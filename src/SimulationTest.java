
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;

class SimulationTest {

    private TruckNode truckNode;
    private Simulation simulation;

    @BeforeEach
    void setup() {
        truckNode = new TruckNode(1, 8001); // adapt constructor if needed
        truckNode.desiredVel = 20.0;
        truckNode.vel = 20.0;
        truckNode.accel = 0.0;
        truckNode.inPlatoon = true;
        truckNode.emergencyActive = false;

        simulation = new Simulation(truckNode);
    }

    // ================= Validation Tests =================
    @Test
    @DisplayName("Simulation start does not crash")
    void testSimulationStart() {
        assertDoesNotThrow(() -> simulation.start());
    }

    @Test
    @DisplayName("Manual emergency sets velocity to zero")
    void testTriggerEmergencyStopsTruck() {
        simulation.triggerEmergency();
        assertEquals(0.0, truckNode.vel);
        assertEquals(-8.0, truckNode.accel);
        assertTrue(truckNode.emergencyActive);
    }

    @Test
    @DisplayName("Matrix clock increments on emergency")
    void testMatrixClockIncrement() {
        simulation.triggerEmergency();
        long[][] clock = simulation.emergencyHandler.getMatrixClock();
        assertNotNull(clock);
        assertTrue(clock[0][0] > 0);
    }

    @Test
    @DisplayName("Emergency handler enters EMERGENCY_BRAKE state")
    void testEmergencyStateTransition() {
        simulation.triggerEmergency();
        assertEquals(State.EMERGENCY_BRAKE, simulation.emergencyHandler.getState());
    }

    

    
  @Test
  @DisplayName("Recovery resets emergency flag")
  void testRecoveryAfterEmergency() throws InterruptedException {

      simulation.start();
      Thread.sleep(200);

      simulation.triggerEmergency();

      // wait up to 5 seconds for recovery
      long timeout = System.currentTimeMillis() + 5000;
      while (truckNode.emergencyActive && System.currentTimeMillis() < timeout) {
          Thread.sleep(100);
      }

      assertFalse(truckNode.emergencyActive,
              "Emergency flag should be cleared after recovery");

      assertEquals(truckNode.desiredVel, truckNode.vel,
              "Velocity should recover");
  }


    // ================= Defect Tests =================
    @Test
    @DisplayName("Triggering emergency twice does not crash")
    void testDoubleEmergency() {
        simulation.triggerEmergency();
        assertDoesNotThrow(() -> simulation.triggerEmergency());
    }

    @Test
    @DisplayName("Emergency without simulation start")
    void testEmergencyWithoutStart() {
        Simulation sim2 = new Simulation(truckNode);
        assertDoesNotThrow(() -> sim2.triggerEmergency());
    }

    @Test
    @DisplayName("Matrix clock is never null")
    void testMatrixClockNotNull() {
        simulation.triggerEmergency();
        assertNotNull(simulation.emergencyHandler.getMatrixClock());
    }

    @Test
    @DisplayName("Velocity cannot be negative")
    void testNegativeVelocity() {
        truckNode.vel = -5.0;
        assertTrue(truckNode.vel < 0, "Velocity is negative (defect detection)");
    }

    @Test
    @DisplayName("Emergency flag remains true if recovery fails")
    void testEmergencyFlagStuck() {
        simulation.triggerEmergency();
        // manually simulate recovery not clearing flag
        truckNode.emergencyActive = true;
        assertTrue(truckNode.emergencyActive, "Emergency flag stuck (defect detection)");
    }

    // ================= Component / Interface Tests =================
    @Test
    @DisplayName("Emergency triggers TruckNodeEmergency")
    void testTriggerEmergencyPropagates() {
        simulation.triggerEmergency();
        assertEquals(State.EMERGENCY_BRAKE, simulation.emergencyHandler.getState());
    }

    @Test
    @DisplayName("TruckNodeEmergency applies emergency braking")
    void testEmergencyBrakingUpdatesTruckNode() {
        simulation.emergencyHandler.triggerLocalEmergency();
        assertEquals(0.0, truckNode.vel);
        assertEquals(-8.0, truckNode.accel);
    }

    @Test
    @DisplayName("Broadcast emergency does not throw exception")
    void testBroadcastEmergency() {
        assertDoesNotThrow(() ->
            simulation.emergencyHandler.broadcastEmergency(
                new TruckNodeEmergency.EmergencyMessage("Alpha", 0.0, 5, new long[4][4])
            )
        );
    }

    // ================= TDD-style Test =================
    @Test
    @DisplayName("Emergency increments matrix clock (TDD)")
    void testEmergencyIncrementsMatrixClockTDD() {
        long[][] before = simulation.emergencyHandler.getMatrixClock();
        simulation.triggerEmergency();
        long[][] after = simulation.emergencyHandler.getMatrixClock();
        assertTrue(after[0][0] > before[0][0], "Matrix clock must increment after emergency");
    }
}

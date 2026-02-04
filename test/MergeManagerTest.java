import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergeManagerTest {

    private TruckNode mergeTruck;
    private MergeManager mergeManager;

    @BeforeEach
    void setup() {
        // Truck 4 = independent truck joining platoon
        mergeTruck = new TruckNode(4, 8004);
        mergeManager = mergeTruck.getMergeManager();
    }

    /**
     * SCENARIO 1:
     * Successful Join-In-Motion
     */
    @Test
    void testJoinInMotionSuccess() {

        // WHEN
        mergeManager.initiateMerge();
        mergeManager.handleMergeMessage("MERGE_SLOT_ASSIGN");
        mergeManager.handleMergeMessage("MERGE_COMMIT");

        // THEN
        assertEquals(
            MergeState.MERGED,
            mergeManager.getState(),
            "Truck should reach MERGED state"
        );

        assertEquals(
            0.6,
            mergeTruck.getGap(),
            0.01,
            "Gap must be restored after successful merge"
        );
    }

    /**
     * SCENARIO 2:
     * Negotiation timeout → safe abort
     */
    @Test
    void testJoinInMotionTimeoutAbort() throws InterruptedException {

        // WHEN
        mergeManager.initiateMerge();

        // Simulate timeout
        Thread.sleep(3100);
        mergeManager.periodicCheck();

        // THEN
        assertEquals(
            MergeState.IDLE,
            mergeManager.getState(),
            "Merge should return to IDLE after timeout"
        );

        assertEquals(
            0.6,
            mergeTruck.getGap(),
            0.01,
            "Gap must be restored after abort"
        );
    }

    /**
     * SCENARIO 3:
     * Communication failure during ALIGNING
     */
    @Test
    void testCommFailureDuringMerge() {

        // GIVEN
        mergeManager.initiateMerge();
        mergeManager.handleMergeMessage("MERGE_SLOT_ASSIGN");

        // WHEN
        mergeManager.handleMergeMessage("MERGE_ABORT");

        // THEN
        assertEquals(
            MergeState.IDLE,
            mergeManager.getState(),
            "Merge must abort safely"
        );

        assertTrue(
            mergeTruck.getGap() >= 0.6,
            "Safety gap must be maintained after abort"
        );
    }
    /**
     * SCENARIO 4:
     * testSpeedAdjustmentDuringMerge
     */
    @Test
    void testSpeedAdjustmentDuringMerge() {

        double initialSpeed = mergeTruck.getVel();

        // Start merge
        mergeManager.initiateMerge();

        // Leader prepares merge
        mergeManager.handleMergeMessage("MERGE_PREPARE");

        // THEN: speed must be reduced
        assertTrue(
            mergeTruck.getDesiredVel() < initialSpeed,
            "Truck speed should be reduced during merge preparation"
        );
    }
    /**
     * SCENARIO 5:
     * testSpeedRestoredAfterMerge
     */
    @Test
    void testSpeedRestoredAfterMerge() {

        mergeManager.initiateMerge();
        mergeManager.handleMergeMessage("MERGE_PREPARE");
        mergeManager.handleMergeMessage("MERGE_SLOT_ASSIGN");
        mergeManager.handleMergeMessage("MERGE_COMMIT");

        assertEquals(
            mergeTruck.getVel(),
            mergeTruck.getDesiredVel(),
            0.01,
            "Speed should be restored after merge completion"
        );

        assertEquals(
            MergeState.MERGED,
            mergeManager.getState(),
            "Truck must reach MERGED state"
        );
    }

}



public class MergeManager {

    private MergeState state = MergeState.IDLE;
    private final TruckNode truck;
    private long stateStartTime;

    private static final long NEGOTIATION_TIMEOUT_MS = 3000;

    public MergeManager(TruckNode truck) {
        this.truck = truck;
    }

    // ===============================
    // MERGE INITIATION
    // ===============================

    public void initiateMerge() {
        if (state != MergeState.IDLE) return;

        state = MergeState.NEGOTIATING;
        stateStartTime = System.currentTimeMillis();

        System.out.println(
            "[MergeManager] Truck " + truck.getTruckId()
            + " sending JOIN-IN-MOTION request"
        );
        
        truck.sendToSpecific(
            truck.getCurrentLeaderId(),
            "MERGE_JOIN_REQ|" + truck.getTruckId() + "|" + truck.getMatrixClock().serialize()
        );
    }

    // ===============================
    // MESSAGE HANDLING
    // ===============================

    public void handleMergeMessage(String msgType) {

        switch (msgType) {

            case "MERGE_SLOT_ASSIGN":
                if (state == MergeState.NEGOTIATING) {
                    state = MergeState.ALIGNING;
                    System.out.println("[MergeManager] Slot assigned");
                }
                break;
            case "MERGE_PREPARE":
                if (state == MergeState.NEGOTIATING) {
                    System.out.println("[MergeManager] Preparing for merge: adjusting speed");
                    truck.adjustSpeedForMerge();
                }
                break;

            case "MERGE_COMMIT":
                if (state == MergeState.ALIGNING) {
                	truck.restoreSpeedAfterMerge();
                    state = MergeState.MERGED;
                    System.out.println("[MergeManager] MERGE COMPLETE");
                }
                break;
            

            case "MERGE_ABORT":
                abortMerge("Leader rejected merge");
                break;

            default:
                break;
        }
    }


    // ===============================
    // TIMEOUT HANDLING
    // ===============================

    public void periodicCheck() {
        if (state == MergeState.NEGOTIATING) {
            if (System.currentTimeMillis() - stateStartTime
                    > NEGOTIATION_TIMEOUT_MS) {
                abortMerge("Negotiation timeout");
            }
        }
    }

    private void abortMerge(String reason) {
        System.out.println("[MergeManager] MERGE ABORTED: " + reason);
        truck.restoreGapAfterMerge();  
        state = MergeState.IDLE;
    }

    public MergeState getState() {
        return state;
    }
}

public class TestCommLoss {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting automated CommLoss test...\n");

       
        System.setProperty("comm.shortMs", "2000");
        System.setProperty("comm.longMs", "4000");
        System.setProperty("comm.extraGap", "2.0");

        
        TruckNode leader = new TruckNode(1, 8001);
        TruckNode follower = new TruckNode(2, 8002);

        
        leader.start();
        follower.start();

        Thread.sleep(1500);

        System.out.println("--- STEP: Verify normal operation (no loss) ---");
        System.out.println("Leader broadcasting speed=25.0 (reliable)");
        leader.broadcastSpeed(25.0);
        Thread.sleep(1000);
        System.out.println("Follower desiredVel: " + follower.getDesiredVel());

        System.out.println("\n--- STEP: Inject packet loss on follower (0.8 -> DEGRADED) ---");
        follower.getCommManager().setLossRate(0.8);
        System.out.println("Set follower LOSS=0.8");

      
        Thread.sleep(3000);
        System.out.println("CM (follower): state=" + follower.getCommManager().getStateName() + ", recentLoss=" + follower.getCommManager().getRecentLossRate());

        System.out.println("\n--- STEP: Leader attempts reliable gap broadcast (should retry) ---");
        leader.broadcastGap(1.5);
        Thread.sleep(2000);
        System.out.println("Follower gap: " + follower.getGap());

        System.out.println("\n--- STEP: Prolonged loss (leader election expected) ---");
        	
        
        Thread.sleep(3000);

        System.out.println("\n--- STEP: Recover (clear loss) ---");
        follower.getCommManager().setLossRate(0.0);
        Thread.sleep(1500);
        System.out.println("CM (follower): state=" + follower.getCommManager().getStateName() + ", totalDegradedMs=" + follower.getCommManager().getTotalDegradedTimeMs());

        System.out.println("\nAutomated test complete. Check console logs above for LOSS_DETECT / PROLONGED_LOSS / LOSS_RECOVERY messages.");
    }
}

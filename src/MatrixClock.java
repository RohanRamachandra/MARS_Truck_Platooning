public class MatrixClock {

    private final int size;
    private final int id;
    private final int[][] clock;
    private final long[][] longClock;
    private final boolean isWrappingLong;

    public MatrixClock(int size, int id) {
        this.size = size;
        this.id = id;
        this.clock = new int[size][size];
        this.longClock = null;
        this.isWrappingLong = false;
    }
    
    /**
     * Constructor for wrapping a long[][] clock matrix from TruckNode.
     * Used to provide a MatrixClock interface over TruckNode's internal clock.
     */
    public MatrixClock(long[][] longClockMatrix, int id) {
        this.size = longClockMatrix.length - 1; // -1 because TruckNode uses 1-based indexing
        this.id = id;
        this.clock = null;
        this.longClock = longClockMatrix;
        this.isWrappingLong = true;
    }

    // ===============================
    // PRINT MATRIX CLOCK
    // ===============================
    public synchronized void print() {
        // Header (best-effort)
        StringBuilder header = new StringBuilder("    ");
        for (int h = 1; h <= size; h++) header.append(" T").append(h);
        System.out.println(header.toString());
        if (isWrappingLong) {
            synchronized (longClock) {
                for (int i = 1; i <= size; i++) {
                    System.out.print("T" + i + " [ ");
                    for (int j = 1; j <= size; j++) {
                        System.out.printf("%2d ", longClock[i][j]);
                    }
                    System.out.println("]");
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                System.out.print("T" + (i + 1) + " [ ");
                for (int j = 0; j < size; j++) {
                    System.out.printf("%2d ", clock[i][j]);
                }
                System.out.println("]");
            }
        }
    }

    // ===============================
    // LOCAL EVENT
    // ===============================
    public synchronized void tick() {
        if (isWrappingLong) {
            synchronized (longClock) {
                if (id >= 1 && id <= size) longClock[id][id]++;
            }
        } else {
            clock[id - 1][id - 1]++;
        }
    }

    // ===============================
    // ON SEND
    // ===============================
    public synchronized int[][] snapshot() {
        if (isWrappingLong) {
            // For wrapped long clock, return a copy without ticking
            int[][] copy = new int[size][size];
            synchronized (longClock) {
                for (int i = 1; i <= size; i++) {
                    for (int j = 1; j <= size; j++) {
                        copy[i-1][j-1] = (int) longClock[i][j];
                    }
                }
            }
            return copy;
        } else {
            // Original behavior for int[][] clock
            tick();
            int[][] copy = new int[size][size];
            for (int i = 0; i < size; i++)
                System.arraycopy(clock[i], 0, copy[i], 0, size);
            return copy;
        }
    }

    // ===============================
    // ON RECEIVE
    // ===============================
    public synchronized void update(int senderId, int[][] received) {
        int s = senderId - 1;
        if (isWrappingLong) {
            synchronized (longClock) {
                // merge sender row into our id row
                for (int i = 1; i <= size; i++) {
                    long val = received[s][i-1];
                    longClock[id][i] = Math.max(longClock[id][i], val);
                }
                // merge full matrix
                for (int i = 1; i <= size; i++) {
                    for (int j = 1; j <= size; j++) {
                        longClock[i][j] = Math.max(longClock[i][j], (long)received[i-1][j-1]);
                    }
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                clock[id - 1][i] = Math.max(clock[id - 1][i], received[s][i]);
            }
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    clock[i][j] = Math.max(clock[i][j], received[i][j]);
                }
            }
        }
    }

    public synchronized String serialize() {
        StringBuilder sb = new StringBuilder();
        if (isWrappingLong) {
            // Serialize from long[][] clock matrix
            synchronized (longClock) {
                for (int i = 1; i <= size; i++) {
                    for (int j = 1; j <= size; j++) {
                        sb.append(longClock[i][j]).append(",");
                    }
                }
            }
        } else {
            // Serialize from int[][] clock matrix
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    sb.append(clock[i][j]).append(",");
                }
            }
        }
        return sb.toString();
    }

    public static int[][] deserialize(String s, int size) {
        int[][] m = new int[size][size];
        String[] p = s.split(",");
        int k = 0;
        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                m[i][j] = Integer.parseInt(p[k++]);
        return m;
    }
}

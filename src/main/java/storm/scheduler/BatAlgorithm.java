package storm.scheduler;

/**
 * Created by Ping on 2018/7/12.
 */
import java.lang.reflect.Array;
import java.util.*;
import java.lang.Math;

public class BatAlgorithm {

    private double[][] X; 		// Population/Solution (N x D)
    private double[][] V; 		// Velocities (N x D)
    private double[][] Q; 		// Frequency : 0 to Q_MAX (N x 1)
    private double[] F;			// Fitness (N)
    private double R; 			// Pulse Rate : 0 to 1
    private double A; 			// Louadness : A_MIN to A_MAX
    private int[][] lb;		// Lower bound (1 x D)
    private int[][] ub;		// Upper bound (1 x D)
    private double fmin; 		// Minimum fitness from F
    private double[] B;			// Best solution array from X (D)

    private final int N; 		// Number of bats
    private final int MAX; 		// Number of iterations
    private final double Q_MIN = 0.0;
    private final double Q_MAX = 2.0;
    private final double A_MIN;
    private final double A_MAX;
    private final double R_MIN;
    private final double R_MAX;
    private final int D = 10;  //dim
    private final Random rand = new Random();

    public BatAlgorithm(int N, int MAX, double A_MIN, double A_MAX, double R_MIN, double R_MAX){
        this.N = N;
        this.MAX = MAX;
        this.R_MAX = R_MAX;
        this.R_MIN = R_MIN;
        this.A_MAX = A_MAX;
        this.A_MIN = A_MIN;

        this.X = new double[N][D];
        this.V = new double[N][D];
        this.Q = new double[N][1];
        this.F = new double[N];
        this.R = (R_MAX + R_MIN) / 2;
        this.A = (A_MIN + A_MAX) / 2;

        // Initialize bounds
        this.lb = new int[1][D];
        for ( int i = 0; i < D; i++ ){
            this.lb[0][i] = 1;
            //System.out.println(lb[0][i]);
        }
        this.ub = new int[1][D];
        for ( int i = 0; i < D; i++ ){
            this.ub[0][i] = 10;
        }

        // Initialize Q and V
        for ( int i = 0; i < N; i++ ){
            this.Q[i][0] = 0.0;
        }
        for ( int i = 0; i < N; i++ ){
            for ( int j = 0; j < D; j++ ) {
                this.V[i][j] = 0.0;
            }
        }

        // Initialize X
        //HashSet executors = new HashSet();
        for ( int i = 0; i < N; i++ ){
            for ( int j = 0; j < D; j++ ){
                this.X[i][j] = lb[0][j] + (int)((ub[0][j] - lb[0][j]) * Math.random());
                //System.out.println(Arrays.toString(X[i]));

            }
            this.F[i] = objective(X[i]);
            System.out.println("初始适应度："+F[i]);
        }
        //System.out.println(Arrays.toString(F));
        //System.out.println(F[1]);
        // Find initial best solution
        int fmin_i = 0;
        for ( int i = 0; i < N; i++ ){
            if ( F[i] < F[fmin_i] )
                fmin_i = i;

        }
        //System.out.println(F[fmin_i]);

        // Store minimum fitness and it's index.
        // B holds the best solution array[1xD]
        this.fmin = F[fmin_i];
        System.out.println("初始最小适应度"+fmin);
        this.B = X[fmin_i]; // (1xD)
        System.out.println("初始最优解"+Arrays.toString(B));
    }

    private double objective(double[] X){
        double time = 0.0;
        double LB = 0.0;
        double CPU = 100+((500 - 100) * rand.nextInt(10));
        //System.out.println(CPU);
        for ( int i = 0; i < D; i++ ){
            time += D/CPU;
            //System.out.println(time);
        }
        for (int i = 0; i < D ; i++) {
            LB = Math.sqrt((Math.pow(time-(time/D),2))/D);
            //System.out.println(LB);

        }

        return LB;
    }

    private double[] simpleBounds(double[] Xi){
        // Don't know if this should be implemented
        double[] Xi_temp = new double[D];
        System.arraycopy(Xi, 0, Xi_temp, 0, D);

        for ( int i = 0; i < D; i++ ){
            if ( Xi_temp[i] < lb[0][i] )
                Xi_temp[i] = lb[0][i];
            else continue;
        }

        for ( int i = 0; i < D; i++ ){
            if ( Xi_temp[i] > ub[0][i] )
                Xi_temp[i] = lb[0][i];
            else continue;
        }
        return Xi_temp;
    }

    private void startBat(){

        double[][] S = new double[N][D];
        int n_iter = 0;
        System.out.println(fmin);
//        System.out.println(Arrays.toString(X));
//        System.out.println(Arrays.toString(B));
        System.out.println("初始的适应度"+Arrays.toString(F));
        // Loop for all iterations/generations(MAX)
        for ( int t = 0; t < MAX; t++ ){
            // Loop for all bats(N)
            for ( int i = 0; i < N; i++ ){

                // Update frequency (Nx1)
                Q[i][0] = Math.abs(Q_MIN + (Q_MIN-Q_MAX) * rand.nextDouble());
                System.out.println(Q[i][0]);
                // Update velocity (NxD)
                for ( int j = 0; j < D; j++ ){
                    //System.out.println(X[i][j] - B[j]);
                    V[i][j] = Math.abs(V[i][j] + (X[i][j] - B[j]) * Q[i][0]);
                    //System.out.println((X[i][j] - B[j]));
                }
                // Update S = X + V
                for ( int j = 0; j < D; j++ ){
                    S[i][j] = X[i][j] + V[i][j];
                    //System.out.println(Arrays.toString(S[i]));
                }
                // Apply bounds/limits
                X[i] = simpleBounds(X[i]);
                //System.out.println(Arrays.toString(X[i]));
                // Pulse rate
                if ( Math.random() > R )
                    for ( int j = 0; j < D; j++ ) {
                        X[i][j] = B[j] + 0.001 * rand.nextGaussian();
                        //System.out.println(Arrays.toString(X[i]));
                    }

                // Evaluate new solutions
                double fnew = objective(X[i]);
                //System.out.println(fnew);
                //System.out.println(F[i]);
                //System.out.println(Arrays.toString(F));

                // Update if the solution improves or not too loud
                if ( fnew <= F[i] && Math.random() < A ){
                    X[i] = S[i];
                    F[i] = fnew;
                    //System.out.println(F[i]);
                }
//                System.out.println(fmin);
//                System.out.println(fnew);
                // Update the current best solution
                if ( fnew <= fmin ){
                    B = X[i];
                    fmin = fnew;
                }
            } // end loop for N
            n_iter = n_iter + N;
        } // end loop for MAX
        List<Double> executors = new ArrayList<Double>();
        // executors = new HashSet();
        for (int i = 0; i <B.length ; i++) {
            //System.out.println(B[i]);
            if (Math.rint((int)B[i])<10&&Math.rint((int)B[i])>0){
                executors.add(Math.rint((int)B[i]));
                System.out.println(executors);
            }

        }

        System.out.println("Number of evaluations : " + n_iter );
        System.out.println("Best = " + executors );
        System.out.println("fmin = " + fmin );
    }

    public static void main(String[] args) {
        new BatAlgorithm(1, 1000, 0.0, 1.0, 0.0, 1.0).startBat();
    }
}

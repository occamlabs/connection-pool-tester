package nl.idgis;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.deegree.db.ConnectionProvider;
import org.deegree.geometry.Envelope;

public class TestExecutor {

    public void doTests( final ConnectionProvider connProvider, final List<Envelope> boxes, final List<String> stmts,
                         final int numThreads )
                            throws InterruptedException {
        ExecutorService executor = newFixedThreadPool( numThreads );
        int id = 1;
        for ( Envelope box : boxes ) {
            TestRun testRun = new TestRun( id++, connProvider, box, stmts );
            executor.execute( testRun );
        }
        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.DAYS );
    }

}

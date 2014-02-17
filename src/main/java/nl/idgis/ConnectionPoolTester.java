package nl.idgis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.deegree.commons.config.DeegreeWorkspace;
import org.deegree.db.ConnectionProvider;
import org.deegree.db.ConnectionProviderProvider;
import org.deegree.geometry.Envelope;
import org.deegree.workspace.Workspace;

public class ConnectionPoolTester {

    public static void main( String[] args )
                            throws IOException, InterruptedException {
        if ( args.length != 5 ) {
            System.out.println( "USAGE: ConnectionPoolTester <workspaceId> <connectionProviderId> <numThreads> <statementsFile> <bboxCsvFile>" );
            System.exit( 0 );
        }

        String workspaceId = args[0];
        Workspace ws = DeegreeWorkspace.getInstance( workspaceId ).getNewWorkspace();
        System.out.println( "- Initializing deegree workspace at '" + workspaceId + "'..." );
        ws.initAll();

        String connectionProviderId = args[1];
        System.out.print( "- Retrieving connection provider '" + connectionProviderId + "'..." );
        final ConnectionProvider connProvider = ws.getResource( ConnectionProviderProvider.class, connectionProviderId );
        if ( connProvider == null ) {
            System.out.println( "Error." );
            System.exit( 0 );
        }
        System.out.println( "OK." );

        final String statementsFile = args[3];
        System.out.print( "- Loading statements from '" + statementsFile + "'..." );
        final List<String> stmts = loadStatements( statementsFile );
        System.out.println( "loaded " + stmts.size() + " statement(s)." );

        final String bboxCsvFile = args[4];
        System.out.print( "- Loading bounding boxes from '" + bboxCsvFile + "'..." );
        final EnvelopeCsvFileLoader fileLoader = new EnvelopeCsvFileLoader();
        final List<Envelope> boxes = fileLoader.load( new File( bboxCsvFile ) );
        System.out.println( "loaded " + boxes.size() + " envelope(s)." );

        final int numThreads = Integer.parseInt( args[2] );
        TestExecutor executor = new TestExecutor();
        long before = System.currentTimeMillis();
        executor.doTests( connProvider, boxes, stmts, numThreads );
        long elapsedMillis = System.currentTimeMillis() - before;
        System.out.println("Total test time: " + elapsedMillis + " [ms] / per test: " + elapsedMillis / boxes.size() + " [ms]");
    }

    private static List<String> loadStatements( String statementsFile )
                            throws IOException {
        final List<String> lines = FileUtils.readLines( new File( statementsFile ) );
        final List<String> stmts = new ArrayList<String>();
        for ( String line : lines ) {
            if ( !line.trim().isEmpty() ) {
                stmts.add( line.trim() );
            }
        }
        return stmts;
    }
}

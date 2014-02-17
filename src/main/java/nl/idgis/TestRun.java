package nl.idgis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

import org.deegree.db.ConnectionProvider;
import org.deegree.geometry.Envelope;
import org.deegree.sqldialect.oracle.sdo.SDOGeometryConverter;

public class TestRun implements Runnable {

    private static final int SRID = 90112;

    private final int id;

    private final ConnectionProvider connProvider;

    private final Envelope env;

    private final List<String> stmts;

    public TestRun( int id, final ConnectionProvider connProvider, final Envelope env, final List<String> stmts ) {
        this.id = id;
        this.connProvider = connProvider;
        this.env = env;
        this.stmts = stmts;
    }

    @Override
    public void run() {
        System.out.println( "- TestRun " + id + ": starting." );
        System.out.println( "- TestRun " + id + ": retrieving connection." );
        final long before = System.currentTimeMillis();
        final Connection conn = connProvider.getConnection();
        final long retrievingConnectionMillis = System.currentTimeMillis() - before;
        System.out.println( "- TestRun " + id + ": retrieving connection took: " + retrievingConnectionMillis + " [ms]" );
        System.out.println( "- TestRun " + id + ": executing " + stmts.size() + " queries" );
        executeQueries( conn );
        System.out.println( "- TestRun " + id + ": closing connection: " + conn );
        try {
            conn.close();
        } catch ( SQLException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long elapsed = System.currentTimeMillis() - before;
        System.out.println( "- TestRun " + id + ": ending, elapsed: " + elapsed + " [ms]" );
    }

    private void executeQueries( final Connection conn ) {
        int queryNo = 1;
        for ( String sql : stmts ) {
            queryNo = executeQuery( conn, queryNo, sql );
        }
    }

    private int executeQuery( final Connection conn, int queryNo, String sql ) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            long beforeMillis = System.currentTimeMillis();
            pstmt = prepareStatementWithBoundingBox( conn, sql );
            pstmt.setFetchSize( 1000 );
            final long preparationMillis = System.currentTimeMillis() - beforeMillis;
            beforeMillis = System.currentTimeMillis();
            rs = pstmt.executeQuery();
            final long executionMillis = System.currentTimeMillis() - beforeMillis;
            beforeMillis = System.currentTimeMillis();
            int rows = fetchResultSet( rs );
            final long fetchingMillis = System.currentTimeMillis() - beforeMillis;
            System.out.println( "- TestRun " + id + ": performed query " + queryNo++ + ": " + rows
                                + " row(s), elapsed: " + preparationMillis + "/" + executionMillis + "/"
                                + fetchingMillis + " [ms]" );
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            closeResultSetAndStatement( pstmt, rs );
        }
        return queryNo;
    }

    private void closeResultSetAndStatement( PreparedStatement pstmt, ResultSet rs ) {
        try {
            rs.close();
            pstmt.close();
        } catch ( SQLException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private int fetchResultSet( ResultSet rs )
                            throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int rows = 0;
        while ( rs.next() ) {
            for ( int i = 1; i <= md.getColumnCount(); i++ ) {
                rs.getObject( i );
            }
            rows++;
        }
        return rows;
    }

    private PreparedStatement prepareStatementWithBoundingBox( final Connection conn, final String sql )
                            throws SQLException {
        String sql2 = sql.replace( ":gtype1", "?" );
        sql2 = sql2.replace( ":srid1", "?" );
        sql2 = sql2.replace( ":elem_info1", "?" );
        sql2 = sql2.replace( ":ordinates1", "?" );
        final PreparedStatement pstmt = conn.prepareStatement( sql );
        int i = 1;
        if ( sql.contains( "?" ) ) {
            Object o = getEnvelopeAsOracleStruct( conn );
            pstmt.setObject( 1, o );
        } else {
            if ( sql.contains( ":gtype1" ) ) {
                pstmt.setInt( i++, 2003 );
            }
            if ( sql.contains( ":srid1" ) ) {
                pstmt.setInt( i++, SRID );
            }
            if ( sql.contains( ":elem_info1" ) ) {
                pstmt.setArray( i++, getElemInfoForEnvelope( conn ) );
            }
            if ( sql.contains( ":ordinates1" ) ) {
                pstmt.setArray( i++, getOrdinateArrayForEnvelope( conn ));
            }
        }
        return pstmt;
    }

    private Object getEnvelopeAsOracleStruct( final Connection conn )
                            throws SQLException {
        OracleConnection ocon = getOracleConnection( conn );
        SDOGeometryConverter sdoConverter = new SDOGeometryConverter();
        Object struct = sdoConverter.fromGeometry( ocon, SRID, env, true );
        return struct;
    }

    private OracleConnection getOracleConnection( final Connection conn )
                            throws SQLException {
        if ( conn instanceof OracleConnection ) {
            return (OracleConnection) conn;
        }
        return conn.unwrap( OracleConnection.class );
    }

    private ARRAY getElemInfoForEnvelope( final Connection conn )
                            throws SQLException {
        final ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor( "MDSYS.SDO_ELEM_INFO_ARRAY", conn );
        final Integer[] elements = new Integer[] { 1, 1003, 3 };
        return new ARRAY( descriptor, conn, elements );
    }

    private ARRAY getOrdinateArrayForEnvelope( final Connection conn )
                            throws SQLException {
        final ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor( "MDSYS.SDO_ORDINATE_ARRAY", conn );
        final Double[] elements = new Double[] { env.getMin().get0(), env.getMin().get1(), env.getMax().get0(),
                                                env.getMax().get1() };
        return new ARRAY( descriptor, conn, elements );
    }

}

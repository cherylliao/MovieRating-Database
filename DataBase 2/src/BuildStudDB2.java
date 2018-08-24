import java.sql.*;

public class BuildStudDB2 {

   public static void main( String[] args ) {

      String tableName = "student";

      Connection conn = null;
      Statement stmt = null;

      String[] userids = new String[] { "Scott McNealy", "Steve Jobs", "Bill Gates" };
      String[] passwords = new String[] { "lavender", "aqua", "blue" };

      /***********************************************************************
      *  determine if the JDBC driver exists and load it...
      ***********************************************************************/
      System.out.print( "\nLoading JDBC driver...\n\n" );
      try {
         Class.forName("oracle.jdbc.OracleDriver");
         }
      catch(ClassNotFoundException e) {
         System.out.println(e);
         System.exit(1);
         }

      /***********************************************************************
      *  establish a connection to the database...
      ***********************************************************************/
      try {
         System.out.print( "Connecting to DEF database...\n\n" );
         //String url = dataSource + dbName;

         conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");


         /*conn = dbms.equals("localAccess") ? DriverManager.getConnection(url)
            : DriverManager.getConnection(url, userName, password );*/
         System.out.println( "Connected to database DEF..." );

         /***********************************************************************
         *  create an object by which we will pass SQL stmts to the database...
         ***********************************************************************/
         stmt = conn.createStatement();
         }
      catch (SQLException se) {
         System.out.println(se);
         System.exit(1);
         }

      /***********************************************************************
      *  in the event that this table already exists, we want to delete it
      *  and build a new table from scratch... if the table doesn't exist,
      *  an SQLException would be thrown when the DROP TABLE stmt below is
      *  executed. We catch that exception, but we don't need to do anything
      *  because we expect the error to occur if the table doesn't exist...
      ***********************************************************************/
      try {
         String dropString = "DROP TABLE " + tableName;
         stmt.executeUpdate(dropString);
         }
      catch (SQLException se) {/*do nothing*/} // table doesn't exist

      try {
         /***********************************************************************
         *  create the new table...
         ***********************************************************************/
         System.out.print( "Building new " + tableName + " table...\n\n" );
         String createString =
            "CREATE TABLE " + tableName +
            "  (username VARCHAR(128) NOT NULL PRIMARY KEY," +
            "   password VARCHAR(128) )";
         stmt.executeUpdate(createString);

         /***********************************************************************
         *  now populate the table...
         ***********************************************************************/
         System.out.print( "Inserting rows in User table...\n\n" );

		 PreparedStatement updateStudent =
		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ? )" );

		 conn.setAutoCommit(false);
		 for( int i = 0; i < userids.length; i++ ) {
			updateStudent.setString( 1, userids[i] );
            updateStudent.setString( 2, passwords[i] );
            updateStudent.executeUpdate();
            }
         conn.commit();

         /***********************************************************************
         *  finally, display all the rows in the database...
         ***********************************************************************/
         ResultSet rset = stmt.executeQuery( "SELECT * FROM " + tableName );
         while( rset.next() )
            System.out.println( rset.getString("username") + ": " +
               rset.getString("password") );

         rset.close();
         stmt.close();
         conn.close();
         }
      catch (SQLException se) {
         System.out.println( "SQL ERROR: " + se );
         }

   } // end main


  }  // end class

import java.sql.*;
import java.io.*;
import java.util.*;


public class BadProject {
	//main method 
	public static void main( String[] args ) throws FileNotFoundException, SQLException{
		//load the methods to create the tables
		System.out.println("extracting year from title into its own column");
        System.out.println("Table format: moveid-year: title");
		/*extract();*/
		System.out.println("matching each genre to each movie title");
		System.out.println("Table format: moveid-genre: title");
		/*genre();*/
		System.out.println("Ratings with userid-movie as composite key");
		/*ratings();*/
		System.out.println("Users table: ");
		/*users();
		age();
		occupation();*/
	
		Connection conn = null;
	    Statement stmt = null;
	    //determine if jdbc deriver exists and load it...
	    System.out.print( "\nLoading JDBC driver...\n\n" );
	      try {
	         Class.forName("oracle.jdbc.OracleDriver");
	         }
	      catch(ClassNotFoundException e) {
	         System.out.println(e);
	         System.exit(1);
	         }
	      //establish a connection to the dabase...
	      try {
	          System.out.print( "Connecting to DEF database...\n\n" );
	          
              conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");
	          System.out.println( "Connected to database DEF..." );
	          //create object to pass SQL statements . . . 
	          stmt = conn.createStatement();
	         }
	      catch (SQLException se) {
	          System.out.println(se);
	          System.exit(1);
	          }
	     //declare the query as string and execute it
	      String query=
	    		  "Select genre from"+
	      "(select genre,count(*) from"+
	    	"(select genre from Moviegenre where movieid in"+
	      "(select movie from Ratings where rating=5))"+
	    "group by genre order by count(*)desc) where rownum<2";

          System.out.println("What is the highest rated genre? ");
          ResultSet rset = stmt.executeQuery(query);
          //display query results
	            while(rset.next())
	            { System.out.println(rset.getString("genre"));
	            
	            }
        //close database objects and connection
	         rset.close();
	         stmt.close();
	         conn.close();
            }
	//method to create users table
	public static void users() throws FileNotFoundException {	
		String tableName="Users";
	    Connection conn = null;
		Statement stmt = null;
		//scan the file
		Scanner scanner = new Scanner(new File("src/users.csv"));
		//store the results in arraylist
		List<String> lines = new ArrayList<String>();
	
		while(scanner.hasNextLine()){
        	lines.add(scanner.nextLine());
        	
        	}
        scanner.close();
    //assigning values to the attributes
        ArrayList<String> userid =new ArrayList<String>();
        ArrayList<String> gender =new ArrayList<String>();
        ArrayList<String> age = new ArrayList<String>();
        ArrayList<String> occupation = new ArrayList<String>();
        ArrayList<String> zipcode = new ArrayList<String>();
        for(int i=0; i<lines.size(); i++)
        {
        	String list = lines.get(i);
        	String[] str = list.split(",");
        	userid.add(str[0]);
        	gender.add(str[1]);
        	age.add(str[2]);
        	occupation.add(str[3]);
        	zipcode.add(str[4]);
        }
      //load JDBC if it exists
        System.out.print( "\nLoading JDBC driver...\n\n" );
        try {
           Class.forName("oracle.jdbc.OracleDriver");
           }
        catch(ClassNotFoundException e) {
           System.out.println(e);
           System.exit(1);
           }
        try {
            System.out.print( "Connecting to DEF database...\n\n" );
           
          conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");

            System.out.println( "Connected to database DEF..." );
            stmt = conn.createStatement();
        }
     catch (SQLException se) {
        System.out.println(se);
        System.exit(1);
        }
        //drop table if it exists
        try {
            String dropString = "DROP TABLE " + tableName;
            stmt.executeUpdate(dropString);
            }
         catch (SQLException se) {/*do nothing*/} // table doesn't exist
         //create the table
         try {
            System.out.print( "Building new " + tableName + " table...\n\n" );
            String createString =
               "CREATE TABLE " + tableName +
                 " (userid varchar(128) primary key,"+
               " gender VARCHAR(128)," +
               "   age VARCHAR(128),"+
               "   occupation VARCHAR(128),"+
               "   zipcode VARCHAR(128))";
            stmt.executeUpdate(createString);

        //populate the table
         System.out.print( "Inserting rows in Users table...\n\n" );
         PreparedStatement updateusers =
        		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ?, ?, ?, ? )" );

        		 conn.setAutoCommit(false);
        		 for( int i = 0; i < lines.size(); i++ ) {
        			updateusers.setString( 1, userid.get(i) );
                    updateusers.setString( 2, gender.get(i) );
                    updateusers.setString(3, age.get(i));
                    updateusers.setString(4, occupation.get(i));
                    updateusers.setString(5, zipcode.get(i));
                    updateusers.executeUpdate();
                    }
                 conn.commit();
                 //display the table
                 ResultSet rset = stmt.executeQuery( "SELECT* FROM " + tableName );
                 
                 while( rset.next() )
                    System.out.println( rset.getString("userid") + "|" +
                       rset.getString("gender")+ "| " +
                       rset.getString("age")+"|"+rset.getString("occupation")+"|"+
                       rset.getString("zipcode"));
                
                 rset.close();
                 stmt.close();
                 conn.close();
                 }
              catch (SQLException se) {
                 System.out.println( "SQL ERROR: " + se );
                 }
              }//end method
	//method to create ratings table
	public static void ratings() throws FileNotFoundException {	
		String tableName="Ratings";
	    Connection conn = null;
		Statement stmt = null;
		//scan the file
		Scanner scanner = new Scanner(new File("src/ratings.csv"));
		//store the results in arraylist
		List<String> lines = new ArrayList<String>();
	    //scan the file
		while(scanner.hasNextLine()){
        	lines.add(scanner.nextLine());
        	
        	}
        scanner.close();
    //assigning values to the attributes
        ArrayList<String> userid =new ArrayList<String>();
        ArrayList<String> movie =new ArrayList<String>();
        ArrayList<String> rating = new ArrayList<String>();
        ArrayList<String> timestamp = new ArrayList<String>();
        for(int i=0; i<lines.size(); i++)
        {
        	String list = lines.get(i);
        	String[] str = list.split(",");
        	userid.add(str[0]);
        	movie.add(str[1]);
        	rating.add(str[2]);
        	timestamp.add(str[3]);
        }
      //load JDBC if it exists
        System.out.print( "\nLoading JDBC driver...\n\n" );
        try {
           Class.forName("oracle.jdbc.OracleDriver");
           }
        catch(ClassNotFoundException e) {
           System.out.println(e);
           System.exit(1);
           }
        try {
            System.out.print( "Connecting to DEF database...\n\n" );
           
          conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");

            System.out.println( "Connected to database DEF..." );
            stmt = conn.createStatement();
        }
     catch (SQLException se) {
        System.out.println(se);
        System.exit(1);
        }
        //drop table if it exists
        try {
            String dropString = "DROP TABLE " + tableName;
            stmt.executeUpdate(dropString);
            }
         catch (SQLException se) {/*do nothing*/} // table doesn't exist
         //create the table
         try {
            System.out.print( "Building new " + tableName + " table...\n\n" );
            String createString =
               "CREATE TABLE " + tableName +
                 " (userid varchar(128),"+
               " movie VARCHAR(128)," +
               "   rating VARCHAR(128),"+
               "   timestamp VARCHAR(128),"+
               "primary key(userid,movie))";
            stmt.executeUpdate(createString);
         //populate the table
         System.out.print( "Inserting rows in Ratings table...\n\n" );
         PreparedStatement updateratings=
        		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ?, ?, ? )" );
                 conn.setAutoCommit(false);
        		 for( int i = 0; i < lines.size(); i++ ) {
        			updateratings.setString( 1, userid.get(i) );
                    updateratings.setString( 2, movie.get(i) );
                    updateratings.setString(3, rating.get(i));
                    updateratings.setString(4, timestamp.get(i));
                    updateratings.executeUpdate();
                    }
                 conn.commit();
                 //display the table
                 ResultSet rset = stmt.executeQuery( "SELECT* FROM " + tableName );
           
                 while( rset.next() )
                    System.out.println( rset.getString("userid") + "-" +
                       rset.getString("movie")+ "|" +
                       rset.getString("rating")+"|"+rset.getString("timestamp"));
                
                 rset.close();
                 stmt.close();
                 conn.close();
                 }
              catch (SQLException se) {
                 System.out.println( "SQL ERROR: " + se );
                 }

           }//method ends;
//method to display genre individually in its own table
	public static void genre() throws FileNotFoundException {	
		String tableName="moviegenre";
	    Connection conn = null;
		Statement stmt = null;
		//scan the file
		Scanner scanner = new Scanner(new File("src/movies.csv"));
		//store the results in arraylist
		List<String> lines = new ArrayList<String>();
		while(scanner.hasNextLine()){
        	lines.add(scanner.nextLine());
        	}
        scanner.close();
        //declare the attributes and parse the data to find these attributes
        String[] arr = lines.toArray(new String[0]);
        ArrayList<String> movieid =new ArrayList<String>();
        ArrayList<String> title =new ArrayList<String>();
        ArrayList<String> genre = new ArrayList<String>();
        for(int i=0; i<arr.length; i++)
        {
        	movieid.add(arr[i].substring(0, arr[i].indexOf(',')));
        	title.add(arr[i].substring(arr[i].indexOf(',')+1,arr[i].lastIndexOf('(')));
        	genre.add(arr[i].substring(arr[i].lastIndexOf(',')+1));
        }
        //load JDBC if found
        System.out.print( "\nLoading JDBC driver...\n\n" );
        try {
           Class.forName("oracle.jdbc.OracleDriver");
           }
        catch(ClassNotFoundException e) {
           System.out.println(e);
           System.exit(1);
           }
        try {
            System.out.print( "Connecting to DEF database...\n\n" );
            
            conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");
           //establish connection to database
            System.out.println( "Connected to database DEF..." );
            //create object for passing sql statements
            stmt = conn.createStatement();
               }
     catch (SQLException se) {
        System.out.println(se);
        System.exit(1);
        }
        //drop table if it exists
        try {
            String dropString = "DROP TABLE " + tableName;
            
            stmt.executeUpdate(dropString);
           
            }
         catch (SQLException se) {/*do nothing*/} // table doesn't exist

         try {
           //make the new table
            System.out.print( "Building new " + tableName + " table...\n\n" );
            String createString =
               "CREATE TABLE " + tableName +
               "  (movieid VARCHAR(128) ," +
               "   genre VARCHAR(128),"+
               "   title VARCHAR(128), "+
               
               "primary key(movieid,genre))";
            stmt.executeUpdate(createString);
           //populate the table
         System.out.print( "Inserting rows in Moviegenre table...\n\n" );

		 PreparedStatement updatemoviegenre =
		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ?,? )" );

		 conn.setAutoCommit(false);
		 
		 for( int i = 0; i <arr.length; i++ ) {
			 String s =genre.get(i);
			 String[] token = s.split("\\|");
			 try {
			updatemoviegenre.setString( 1, movieid.get(i));
            updatemoviegenre.setString( 2, token[0]);
            updatemoviegenre.setString(3, title.get(i));
            updatemoviegenre.executeUpdate();
            updatemoviegenre.setString( 1, movieid.get(i) );
            updatemoviegenre.setString( 2, token[1] );
            updatemoviegenre.setString(3,title.get(i) );
            updatemoviegenre.executeUpdate();
            updatemoviegenre.setString( 1, movieid.get(i) );
            updatemoviegenre.setString( 2, token[2]);
            updatemoviegenre.setString(3,title.get(i)  );
            updatemoviegenre.executeUpdate();
            updatemoviegenre.setString( 1, movieid.get(i) );
            updatemoviegenre.setString( 2, token[3] );
            updatemoviegenre.setString(3, title.get(i));
            updatemoviegenre.executeUpdate();
            }catch(ArrayIndexOutOfBoundsException ex) {}}

         conn.commit();
         
         //display results
         ResultSet rset = stmt.executeQuery( "SELECT* FROM "+tableName);
         while( rset.next() )
            System.out.println( 
               rset.getString("movieid")+"-"+rset.getString("genre")+"|"+rset.getString("title"));
         rset.close();
         stmt.close();
    
         conn.close();
         }
      catch (SQLException se) {
         System.out.println( "SQL ERROR: " + se );
         }
      }

//method for extracting year from the title and put into its own column 
public static void extract() throws FileNotFoundException{
		String tableName = "movieyear";
		Connection conn = null;
	      Statement stmt = null;
	      //instantiate scanner to read the file 
		Scanner scanner = new Scanner(new File("src/movies.csv"));
        //store content into arraylist
		List<String> lines = new ArrayList<String>();
        while(scanner.hasNextLine()){
        	lines.add(scanner.nextLine());
        	}
        scanner.close();
        //parse the data to desired attributes
        String[] arr = lines.toArray(new String[0]);
        ArrayList<String> movieid =new ArrayList<String>();
        ArrayList<String> title =new ArrayList<String>();
        ArrayList<String> year =new ArrayList<String>();
        for(int i=0; i<arr.length; i++)
        {
        	movieid.add(arr[i].substring(0, arr[i].indexOf(',')));
        	title.add(arr[i].substring(arr[i].indexOf(',')+1,arr[i].lastIndexOf('(')));
        	year.add(arr[i].substring(arr[i].lastIndexOf('(')+1,arr[i].lastIndexOf(')')));
        }
        //load JDBC if it exists
        System.out.print( "\nLoading JDBC driver...\n\n" );
        try {
           Class.forName("oracle.jdbc.OracleDriver");
           }
        catch(ClassNotFoundException e) {
           System.out.println(e);
           System.exit(1);
           }
        try {
            System.out.print( "Connecting to DEF database...\n\n" );
           
          conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");

            System.out.println( "Connected to database DEF..." );
            //create database object
            stmt = conn.createStatement();
        }
     catch (SQLException se) {
        System.out.println(se);
        System.exit(1);
        }
        //drop table if it exists
        try {
            String dropString = "DROP TABLE " + tableName;
            stmt.executeUpdate(dropString);
            }
         catch (SQLException se) {/*do nothing*/} // table doesn't exist
         //create the table
         try {
            System.out.print( "Building new " + tableName + " table...\n\n" );
            String createString =
               "CREATE TABLE " + tableName +
               "  (movieid VARCHAR(128)," +
               "   year VARCHAR(128),"+
               "   title VARCHAR(128),"+
               "primary key(movieid,year))";
            stmt.executeUpdate(createString);

        //populate the table
         System.out.print( "Inserting rows in Movieyear table...\n\n" );

		 PreparedStatement updatemovieyear =
		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ?,? )" );

		 conn.setAutoCommit(false);
		 for( int i = 0; i < arr.length; i++ ) {
			updatemovieyear.setString( 1, movieid.get(i) );
            updatemovieyear.setString( 2, year.get(i) );
            updatemovieyear.setString(3, title.get(i));
            updatemovieyear.executeUpdate();
            }
         conn.commit();
         //display the table
         ResultSet rset = stmt.executeQuery( "SELECT* FROM " + tableName );
         
         while( rset.next() )
            System.out.println( rset.getString("movieid") + "-" +
               rset.getString("year")+ ": " +
               rset.getString("title"));
        
         rset.close();
         stmt.close();
         conn.close();
         }
      catch (SQLException se) {
         System.out.println( "SQL ERROR: " + se );
         }

   } 
    //Age table to decode age information un the User table
	public static void age(){
		String tableName = "age";
		
		Connection conn = null;
	      Statement stmt = null;
          //declare table content as arrays
	      String[] agecode = new String[] { "1", "18", "25","35","45","50","56" };
	      String[] agerange = new String[] { "Under 18", "18-24", "25-34","35-44","45-49",
	    		  "50-55","56+"};
	      //determine if the driver exists and loads it: 
	      System.out.print( "\nLoading JDBC driver...\n\n" );
	      try {
	         Class.forName("oracle.jdbc.OracleDriver");
	         }
	      catch(ClassNotFoundException e) {
	         System.out.println(e);
	         System.exit(1);
	         }
	     //establish a connection to the database...
	      
	      try {
	         System.out.print( "Connecting to DEF database...\n\n" );
	       
 conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");

	         System.out.println( "Connected to database DEF..." );
             //create database object
	         stmt = conn.createStatement();
	         }
	      catch (SQLException se) {
	         System.out.println(se);
	         System.exit(1);
	         }

	     //drop table if it exists
	      try {
	         String dropString = "DROP TABLE " + tableName;
	         stmt.executeUpdate(dropString);
	         }
	      catch (SQLException se) {/*do nothing*/} // table doesn't exist

	      try {
	        //create table
	         System.out.print( "Building new " + tableName + " table...\n\n" );
	         String createString =
	            "CREATE TABLE " + tableName +
	            "  (agecode VARCHAR(128) NOT NULL PRIMARY KEY," +
	            "   agerange VARCHAR(128) )";
	         stmt.executeUpdate(createString);
	        //populate table
	          System.out.print( "Inserting rows in User table...\n\n" );

	 		 PreparedStatement updateAge =
	 		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ? )" );

	 		 conn.setAutoCommit(false);
	 		 for( int i = 0; i < agecode.length; i++ ) {
	 			updateAge.setString( 1, agecode[i] );
	             updateAge.setString( 2, agerange[i] );
	             updateAge.executeUpdate();
	             }
	          conn.commit();
              //display table
	          ResultSet rset = stmt.executeQuery( "SELECT * FROM " + tableName );
	          System.out.println("age table decodes age information in the format of");
	          System.out.println("Agecode: Agerange");
	          while( rset.next() )
	             System.out.println( rset.getString("agecode") + ": " +
	                rset.getString("agerange") );

	          rset.close();
	          stmt.close();
	          conn.close();
	          }
	       catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	          }

	    } // end method;
	//method to create table to decode occupation information in the Users table. 
		public static void occupation() {
			String tableName = "occupation";
			
			Connection conn = null;
		      Statement stmt = null;
             //declare the table content as arrays
		      String[] jobcode = new String[] { "0", "1", "2","3","4","5","6","7","8","9",
		    		  "10","11","12","13","14","15","16","17","18","19","20"};
		      String[] description = new String[] { "other or not specified","academic/educator" , "artist","clerical/admin",
		    		  "college/grad student","customer service","doctor/health care","executive/managerial",
		    		  "farmer","homemaker","K-12 student","lawyer","programmer","retired","sales/marketing",
		    		  "scientist","self-employed","technician/engineer","tradesman/craftsman","unemployed","writer"
		    		  };
		      //determine if the driver exists and loads it: 
		      System.out.print( "\nLoading JDBC driver...\n\n" );
		      try {
		         Class.forName("oracle.jdbc.OracleDriver");
		         }
		      catch(ClassNotFoundException e) {
		         System.out.println(e);
		         System.exit(1);
		         }
		     //establish a connection to the database...
		      
		      try {
		         System.out.print( "Connecting to DEF database...\n\n" );
		         
             conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "dcalasci", "demo");

		         System.out.println( "Connected to database DEF..." );
                 //create database object
		         stmt = conn.createStatement();
		         }
		      catch (SQLException se) {
		         System.out.println(se);
		         System.exit(1);
		         }

		    //drop table if it exists
		      try {
		         String dropString = "DROP TABLE " + tableName;
		         stmt.executeUpdate(dropString);
		         }
		      catch (SQLException se) {/*do nothing*/} // table doesn't exist

		      try {
		         //create table
		         System.out.print( "Building new " + tableName + " table...\n\n" );
		         String createString =
		            "CREATE TABLE " + tableName +
		            "  (jobcode VARCHAR(128) NOT NULL PRIMARY KEY," +
		            "   description VARCHAR(128) )";
		         stmt.executeUpdate(createString);
		         //populate the table
		          System.out.print( "Inserting rows in User table...\n\n" );

		 		 PreparedStatement update =
		 		 conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ? )" );

		 		 conn.setAutoCommit(false);
		 		 for( int i = 0; i < jobcode.length; i++ ) {
		 			update.setString( 1, jobcode[i] );
		             update.setString( 2, description[i] );
		             update.executeUpdate();
		             }
		          conn.commit();
                 //display table content
		          ResultSet rset = stmt.executeQuery( "SELECT * FROM " + tableName );
		          System.out.println("Occupation table decodes occupation information in the format of");
		          System.out.println("Jobcode: Description");
		          while( rset.next() )
		             System.out.println( rset.getString("jobcode") + ": " +
		                rset.getString("description") );

		          rset.close();
		          stmt.close();
		          conn.close();
		          }
		       catch (SQLException se) {
		          System.out.println( "SQL ERROR: " + se );
		          }

		    } // end method;

	   }  // end class


		
		
		
		
		
	



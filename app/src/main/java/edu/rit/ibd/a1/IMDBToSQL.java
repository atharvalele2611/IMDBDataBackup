package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class IMDBToSQL {
	private static final String NULL_STR = "\\N";
	private static final String TAB = "\t";
	private static final String TITLE = "tt";
	private static final String NAME = "nm";
	private static int maxG = 0;
	private static final int batchSize = 1000;

	private static final String DROP_ALL = "DROP TABLE IF EXISTS Movie,Genre,MovieGenre,Person,KnownFor,Actor, Director, Producer,Writer;";

	private static final String CREATE_MOVIE = "CREATE TABLE Movie(id INTEGER, ptitle VARCHAR(?), otitle VARCHAR(?), adult BOOLEAN, year INTEGER , runtime INTEGER, rating FLOAT, totalvotes INTEGER, PRIMARY KEY (id));";
	private static final String CREATE_GENRE = "CREATE TABLE Genre(id Integer AUTO_INCREMENT, name VARCHAR(?), PRIMARY KEY(id), UNIQUE(name));";
	private static final String CREATE_MOVIE_GENRE = "CREATE TABLE MovieGenre(mid Integer, gid Integer, PRIMARY KEY(mid,gid));";
	private static final String CREATE_PERSON = "CREATE TABLE Person(id INTEGER,name VARCHAR(?), byear INTEGER, dyear INTEGER,PRIMARY KEY (id));";
	private static final String CREATE_KNOWNFOR = "CREATE TABLE KnownFor(mid INTEGER, pid INTEGER,PRIMARY KEY (mid,pid));";
	private static final String CREATE_ACTOR = "CREATE TABLE Actor(mid INTEGER, pid INTEGER, PRIMARY KEY (mid,pid))";
	private static final String CREATE_DIRECTOR = "CREATE TABLE Director(mid INTEGER, pid INTEGER, PRIMARY KEY (mid,pid))";
	private static final String CREATE_PRODUCER = "CREATE TABLE Producer(mid INTEGER, pid INTEGER, PRIMARY KEY (mid,pid))";
	private static final String CREATE_WRITER = "CREATE TABLE Writer(mid INTEGER, pid INTEGER, PRIMARY KEY (mid,pid))";

	private static final String INSERT_MOVIE = "INSERT IGNORE INTO Movie(id, ptitle, otitle,adult,year,runtime) VALUES(?,?,?,?,?,?)";
	private static final String INSERT_GENRE = "INSERT INTO Genre(name) SELECT * from (select ? as g) as tmp where not exists (select g from Genre where name = g)";
	private static final String INSERT_MOVIE_GENRE = "INSERT IGNORE INTO MovieGenre(mid, gid) VALUES(?,(SELECT id from Genre where name = ?))";
	private static final String INSERT_PERSON = "INSERT INTO Person(id,name, byear, dyear) VALUES(?,?,?,?)";
	private static final String INSERT_KNOWNFOR = "INSERT INTO KnownFor(mid,pid) VALUES(?,?)";
	private static final String INSERT_ACTOR = "INSERT IGNORE INTO Actor(mid,pid) VALUES(?,?)";
	private static final String INSERT_DIRECTOR = "INSERT IGNORE INTO Director(mid,pid) VALUES(?,?)";
	private static final String INSERT_PRODUCER = "INSERT IGNORE INTO Producer(mid,pid) VALUES(?,?)";
	private static final String INSERT_WRITER = "INSERT IGNORE INTO Writer(mid,pid) VALUES(?,?)";

	private static final String DELETE_FROM_KNOWNFOR = "DELETE FROM KnownFor as k  WHERE NOT EXISTS( (SELECT id from Movie m WHERE k.mid = m.id));";
	private static final String DELETE_FROM_ACTOR_M = "DELETE FROM Actor as a  WHERE NOT EXISTS( (SELECT id from Movie m WHERE a.mid = m.id));";
	private static final String DELETE_FROM_ACTOR_P = "DELETE FROM Actor as a  WHERE NOT EXISTS( (SELECT id from Person p WHERE a.pid = p.id));";
	private static final String DELETE_FROM_DIRECTOR_M = "DELETE FROM Director as d  WHERE NOT EXISTS( (SELECT id from Movie m WHERE d.mid = m.id));";
	private static final String DELETE_FROM_DIRECTOR_P = "DELETE FROM Director as d  WHERE NOT EXISTS( (SELECT id from Person p WHERE d.pid = p.id));";
	private static final String DELETE_FROM_PRODUCER_M = "DELETE FROM Producer as pr  WHERE NOT EXISTS( (SELECT id from Movie m WHERE pr.mid = m.id));";
	private static final String DELETE_FROM_PRODUCER_P = "DELETE FROM Producer as pr  WHERE NOT EXISTS( (SELECT id from Person p WHERE pr.pid = p.id));";
	private static final String DELETE_FROM_WRITER_M = "DELETE FROM Writer as w  WHERE NOT EXISTS( (SELECT id from Movie m WHERE w.mid = m.id));";
	private static final String DELETE_FROM_WRITER_P = "DELETE FROM Writer as w  WHERE NOT EXISTS( (SELECT id from Person p WHERE w.pid = p.id));";

	private static final String UPDATE_RATINGS = "UPDATE Movie SET totalvotes = ?, rating = ? WHERE id = ? ";

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];

		/*
		 * 
		 * (Outside Java) You must install MySQL and create a database user and
		 * password. Then, you must create a schema. You need to grant permissions to
		 * the user over the schema.
		 * 
		 * (In your Java program) Before inserting data, you need to create a relation.
		 * You can create all relations at once or while you are populating the
		 * database. Creating a new relation is as follows: st =
		 * con.prepareStatement("CREATE TABLE X..."); st.execute(); st.close();
		 * 
		 * IMPORTANT: You should never ever use schema names such as "CREATE TABLE
		 * sch.X..." If you do so, you are forcing a database named 'sch' to exist,
		 * which defeats the purpose of receiving the database as an input parameter. I
		 * REPEAT: NEVER USE SCHEMA NAMES IN THIS COURSE!!!!!
		 * 
		 * In this assignment, you will learn how to load data from external files into
		 * a relational database using JDBC. In general, these external files will not
		 * be perfect and contain data issues. When dealing with a relational database,
		 * the most important of these issues are the foreign keys: a piece of data may
		 * not be available in a referenced relation. This actually happens in the IMDB
		 * data so, in order to avoid these issues, you must NOT create foreign keys.
		 * Instead, you need to create the relations without any constraints except for
		 * primary keys. At the end of the process, you must delete all the data that is
		 * not relevant to us, i.e., tuples that point to movies or people that are not
		 * present in the tables must be deleted.
		 * 
		 * It is very important to release resources ASAP, so always close everything as
		 * soon as you are done. While debugging, you may want to delete a previously
		 * created relation, you can do so as follows: st =
		 * con.prepareStatement("DROP TABLE IF EXISTS [TABLE_NAME]");
		 * 
		 * Be careful with this statement as relations will be completely removed,
		 * including the data. Note that, if you have created foreign keys, you cannot
		 * drop a relation that references another, that is, to drop a relation you may
		 * first to drop other relation(s) first.
		 * 
		 * Assuming a relation has been created, you can insert data by reading from the
		 * GZip files as follows: InputStream gzipStream = new GZIPInputStream( new
		 * FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz")); Scanner sc =
		 * new Scanner(gzipStream, "UTF-8"); while (sc.hasNextLine()) { String line =
		 * sc.nextLine(); // Do your amazing stuff. } sc.close();
		 * 
		 * To load data massively, you should use the batch command. First, the JDBC URL
		 * should have a parameter as follows: ?rewriteBatchedStatements=true. You
		 * should not worry about this since the grading software will take care of it.
		 * Then, create an INSERT statement as follows: st =
		 * con.prepareStatement("INSERT INTO ...");
		 * 
		 * There are different options to load the data but the recommended one is to
		 * rely on parameterized queries: st =
		 * con.prepareStatement("INSERT INTO X(a, b, c) VALUES (?,?,?)");
		 * 
		 * Now, you can use st.set[Int/String/Float...](i, v), where i denotes the
		 * position of the question mark starting in 1, and v denotes the specific
		 * value. You need to use the appropriate method according to the type of the
		 * attribute in the relation. Once you are done filling the data for this
		 * specific tuple, you must add the statement to the batch: st.addBatch();
		 * 
		 * This will internally add something like "INSERT INTO X(a, b, c) VALUES
		 * (1,'t','x')" to be executed at a later stage.
		 * 
		 * Remember that, by default, every statement will be executed in isolation,
		 * which defeats our purpose of running multiple statements at a time. To
		 * disable such behavior, you must explicitly invoke the method in the
		 * connection: con.setAutoCommit(false);
		 * 
		 * After this method is executed, changes in the database will not be reflected
		 * unless you call con.commit().
		 * 
		 * All these statements will be stored in main memory. Since there are
		 * restrictions on the amount of main memory you can use, you must commit from
		 * time to time to release memory. I recommend to set a "step" variable that
		 * will control how many statements you will process at a time as follows: int
		 * step = 100; (...) int cnt = 0; while (...) { cnt++; // Keep working on adding
		 * statements to the batch. (...) if (cnt % step == 0) { // Execute all pending
		 * statements. st.executeBatch(); // Commit the changes. con.commit(); // There
		 * are no more statements, new statements will be added. } } // Leftovers!
		 * st.executeBatch(); con.commit(); st.close();
		 * 
		 * Note that the last calls after the loop are necessary to deal with the
		 * leftover statements. The size of the step will determine the amount of memory
		 * you will use. You will generally achieve better performance handling many
		 * statements at once, but you can use more memory than allowed. You can play
		 * around to find the best configuration, but remember that your programs must
		 * work on an external computer that you do not have access to, so it is
		 * generally better to play safe without pushing the limits.
		 * 
		 * Finally, there are certain tuples that are repeated. You should use "INSERT
		 * IGNORE INTO..." to ignore issues with primary keys. The first tuple will be
		 * kept while the rest, duplicates will be ignored without errors.
		 */

		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		con.setAutoCommit(false);

		dropALL(con);

		createMovies(folderToIMDBGZipFiles, con);

		createGenres(folderToIMDBGZipFiles, con);

		createMovieGenres(folderToIMDBGZipFiles, con);

		insertMoviesAndGenres(folderToIMDBGZipFiles, con);

		insertMovieGenre(folderToIMDBGZipFiles, con);

		updateMovieRatings(folderToIMDBGZipFiles, con);

		createPerson(folderToIMDBGZipFiles, con);

		createKnownFor(con);

		insertPersonAndKnownFor(folderToIMDBGZipFiles, con);

		deleteFromKnown(con);

		createActorDirectorProducerWriter(con);

		insertADPW(folderToIMDBGZipFiles, con);

		insertDW(folderToIMDBGZipFiles, con);

		deleteADPW(con);

		con.close();
	}

	private static void createMovies(String folderToIMDBGZipFiles, Connection con) throws Exception {
		InputStream gzipStream = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
		Scanner sc1 = new Scanner(gzipStream, "UTF-8");
		PreparedStatement st = con.prepareStatement(CREATE_MOVIE);
		int maxP = 0;
		int maxO = 0;
		sc1.nextLine();
		while (sc1.hasNextLine()) {
			String line = sc1.nextLine();
			if (maxP < line.split(TAB)[2].length()) {
				maxP = line.split(TAB)[2].length();
			}
			if (maxO < line.split(TAB)[3].length()) {
				maxO = line.split(TAB)[3].length();
			}
			if (maxG < line.split(TAB)[8].length()) {
				maxG = line.split(TAB)[8].length();
			}
		}

		st.setInt(1, maxP + 100);
		st.setInt(2, maxO + 100);
		st.execute();
		con.commit();

		st.close();
		gzipStream.close();
		sc1.close();
	}

	private static void createGenres(String folderToIMDBGZipFiles, Connection con) throws Exception {
		PreparedStatement st = con.prepareStatement(CREATE_GENRE);
		st.setInt(1, maxG);
		st.execute();
		con.commit();

		st.close();
	}

	private static void createMovieGenres(String folderToIMDBGZipFiles, Connection con) throws Exception {
		PreparedStatement st = con.prepareStatement(CREATE_MOVIE_GENRE);
		st.execute();
		con.commit();

		st.close();
	}

	private static void insertMoviesAndGenres(String folderToIMDBGZipFiles, Connection con) throws Exception {
		PreparedStatement st = con.prepareStatement(INSERT_MOVIE);

		PreparedStatement stG = con.prepareStatement(INSERT_GENRE);

		InputStream gzipStream1 = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
		Scanner sc = new Scanner(gzipStream1, "UTF-8");

		if (sc.hasNextLine())
			sc.nextLine();
		int entry = 0;
		while (sc.hasNextLine()) {
			String line = sc.nextLine();

			// Split the line.
			String[] splitLine = line.split(TAB);

			if (splitLine[1].equals("movie")) {
				entry++;
				/* Set movie id with appropriate type (integer). */
				st.setInt(1, Integer.parseInt(splitLine[0].replace(TITLE, ""))); // id

				st.setString(2, splitLine[2]); // ptitle
				st.setString(3, splitLine[3]); // otitle
				st.setBoolean(4, Integer.parseInt(splitLine[4]) == 0 ? false : true); // adult

				if (splitLine[5].equals(NULL_STR))
					st.setNull(5, Types.VARCHAR);
				else
					st.setInt(5, Integer.parseInt(splitLine[5])); // year

				if (splitLine[7].equals(NULL_STR))
					st.setNull(6, Types.VARCHAR);
				else
					st.setInt(6, Integer.parseInt(splitLine[7])); // runtime

				if (splitLine[8].equals(NULL_STR)) {
					stG.setNull(1, Types.VARCHAR);
				} else {
					for (String genre : splitLine[8].split(",")) {
						stG.setString(1, genre); // genre
						stG.addBatch();
					}
				}
				st.addBatch();

				if (entry % batchSize == 0) {
					st.executeBatch();
					stG.executeBatch();
					con.commit();
				}
			}

		}

		st.executeBatch();
		stG.executeBatch();
		con.commit();

		st.close();
		stG.close();
		gzipStream1.close();
		sc.close();
	}

	private static void insertMovieGenre(String folderToIMDBGZipFiles, Connection con) throws Exception {
		PreparedStatement stMG = con.prepareStatement(INSERT_MOVIE_GENRE);

		InputStream gzipStream2 = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));

		Scanner scMG = new Scanner(gzipStream2, "UTF-8");
		if (scMG.hasNextLine())
			scMG.nextLine();
		int entry = 0;
		while (scMG.hasNextLine()) {
			String line = scMG.nextLine();
			String[] splitLine = line.split(TAB);

			if (splitLine[1].equals("movie")) {

				stMG.setInt(1, Integer.parseInt(splitLine[0].replace(TITLE, ""))); // id
				if (splitLine[8].equals(NULL_STR)) {
					stMG.setNull(2, Types.VARCHAR);
				} else {
					for (String genre : splitLine[8].split(",")) {
						entry++;
						stMG.setString(2, genre); // genre
						stMG.addBatch();
					}
				}

				if (entry % batchSize == 0) {
					stMG.executeBatch();
					con.commit();
				}

			}
		}
		stMG.executeBatch();
		con.commit();

		stMG.close();
		scMG.close();
		gzipStream2.close();

	}

	private static void updateMovieRatings(String folderToIMDBGZipFiles, Connection con) throws Exception {
		PreparedStatement stVotes = con.prepareStatement(UPDATE_RATINGS);

		InputStream gzipStreamRatings = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
		Scanner sc2 = new Scanner(gzipStreamRatings, "UTF-8");

		if (sc2.hasNextLine())
			sc2.nextLine();
		int entry = 0;
		while (sc2.hasNextLine()) {
			entry++;
			String line = sc2.nextLine();
			String[] splitLine = line.split(TAB);

			stVotes.setInt(3, Integer.parseInt(splitLine[0].replace(TITLE, ""))); // id
			stVotes.setFloat(2, Float.parseFloat(splitLine[1]));
			stVotes.setInt(1, Integer.parseInt(splitLine[2]));
			stVotes.addBatch();
			if (entry % batchSize == 0) {
				stVotes.executeBatch();
				con.commit();
			}

		}
		sc2.close();
		stVotes.executeBatch();
		con.commit();
		gzipStreamRatings.close();
	}

	private static void createPerson(String folderToIMDBGZipFiles, Connection con) throws Exception {
		InputStream gzipStreamPerson = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
		Scanner sc3 = new Scanner(gzipStreamPerson, "UTF-8");

		PreparedStatement stPerson = con.prepareStatement(CREATE_PERSON);
		int maxName = 0;
		sc3.nextLine();
		while (sc3.hasNextLine()) {
			String line = sc3.nextLine();
			if (maxName < line.split(TAB)[1].length()) {
				maxName = line.split(TAB)[1].length();
			}
		}
		stPerson.setInt(1, maxName + 100);
		stPerson.execute();
		con.commit();

		gzipStreamPerson.close();
		sc3.close();

	}

	private static void createKnownFor(Connection con) throws Exception {
		PreparedStatement stKnownFor = con.prepareStatement(CREATE_KNOWNFOR);
		stKnownFor.execute();
		con.commit();

		stKnownFor.close();

	}

	private static void insertPersonAndKnownFor(String folderToIMDBGZipFiles, Connection con) throws Exception {
		InputStream gzipStreamPerson1 = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
		Scanner sc4 = new Scanner(gzipStreamPerson1, "UTF-8");
		if (sc4.hasNextLine())
			sc4.nextLine();
		int entry = 0;
		PreparedStatement stPerson = con.prepareStatement(INSERT_PERSON);
		PreparedStatement stKnownFor = con.prepareStatement(INSERT_KNOWNFOR);
		while (sc4.hasNextLine()) {
			entry++;
			String line = sc4.nextLine();
			String[] splitLine = line.split(TAB);
			stPerson.setInt(1, Integer.parseInt(splitLine[0].replace(NAME, "")));

			if (splitLine[1].equals(NULL_STR))
				stPerson.setNull(2, Types.VARCHAR);
			else
				stPerson.setString(2, splitLine[1]);

			if (splitLine[2].equals(NULL_STR))
				stPerson.setNull(3, Types.VARCHAR);
			else
				stPerson.setInt(3, Integer.parseInt(splitLine[2]));

			if (splitLine[3].equals(NULL_STR))
				stPerson.setNull(4, Types.VARCHAR);
			else
				stPerson.setInt(4, Integer.parseInt(splitLine[3]));

			if (!splitLine[5].equals(NULL_STR)) {
				stKnownFor.setInt(2, Integer.parseInt(splitLine[0].replace(NAME, "")));
				String[] knownForTittles = splitLine[5].split(",");
				for (String title : knownForTittles) {
					stKnownFor.setInt(1, Integer.parseInt(title.replace(TITLE, "")));
					stKnownFor.addBatch();
				}
			}
			stPerson.addBatch();
			if (entry % batchSize == 0) {
				stPerson.executeBatch();
				con.commit();
				stKnownFor.executeBatch();
				con.commit();

			}
		}
		stPerson.executeBatch();
		con.commit();
		stKnownFor.executeBatch();
		con.commit();

		stPerson.close();
		sc4.close();
		gzipStreamPerson1.close();

	}

	private static void deleteFromKnown(Connection con) throws Exception {
		PreparedStatement stKnownFor = con.prepareStatement(DELETE_FROM_KNOWNFOR);
		stKnownFor.execute();
		con.commit();

		stKnownFor.close();
	}

	private static void createActorDirectorProducerWriter(Connection con) throws Exception {
		PreparedStatement stPrinc = con.prepareStatement(CREATE_ACTOR);
		stPrinc.execute();
		con.commit();

		stPrinc = con.prepareStatement(CREATE_DIRECTOR);
		stPrinc.execute();
		con.commit();

		stPrinc = con.prepareStatement(CREATE_PRODUCER);
		stPrinc.execute();
		con.commit();

		stPrinc = con.prepareStatement(CREATE_WRITER);
		stPrinc.execute();
		con.commit();
		stPrinc.close();
	}

	private static void insertADPW(String folderToIMDBGZipFiles, Connection con) throws Exception {
		InputStream gzipStreamPrinc = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		Scanner sc5 = new Scanner(gzipStreamPrinc, "UTF-8");
		if (sc5.hasNextLine())
			sc5.nextLine();
		int entry = 0;
		PreparedStatement act = con.prepareStatement(INSERT_ACTOR);
		PreparedStatement direct = con.prepareStatement(INSERT_DIRECTOR);
		PreparedStatement prod = con.prepareStatement(INSERT_PRODUCER);
		PreparedStatement writr = con.prepareStatement(INSERT_WRITER);
		while (sc5.hasNextLine()) {

			String line = sc5.nextLine();
			String[] splitLine = line.split(TAB);
			int mid = Integer.parseInt(splitLine[0].replace(TITLE, ""));
			int pid = Integer.parseInt(splitLine[2].replace(NAME, ""));
			entry++;
			String job = splitLine[3].toLowerCase();

			switch (job) {
			case "actor":
				act.setInt(1, mid);
				act.setInt(2, pid);
				act.addBatch();
				break;
			case "actress":
				act.setInt(1, mid);
				act.setInt(2, pid);
				act.addBatch();
				break;
			case "self":
				act.setInt(1, mid);
				act.setInt(2, pid);
				act.addBatch();
				break;
			case "director":
				direct.setInt(1, mid);
				direct.setInt(2, pid);
				direct.addBatch();
				break;
			case "producer":
				prod.setInt(1, mid);
				prod.setInt(2, pid);
				prod.addBatch();
				break;
			case "writer":
				writr.setInt(1, mid);
				writr.setInt(2, pid);
				writr.addBatch();
				break;
			default:
				break;
			}

			if (entry % batchSize == 0) {
				act.executeBatch();
				direct.executeBatch();
				prod.executeBatch();
				writr.executeBatch();
				con.commit();
			}
		}
		act.executeBatch();
		direct.executeBatch();
		prod.executeBatch();
		writr.executeBatch();
		con.commit();

		act.close();
		direct.close();
		prod.close();
		writr.close();
		sc5.close();
		gzipStreamPrinc.close();
	}

	private static void insertDW(String folderToIMDBGZipFiles, Connection con) throws Exception {
		InputStream gzipStreamCrew = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"));
		Scanner scCrw = new Scanner(gzipStreamCrew, "UTF-8");
		if (scCrw.hasNextLine())
			scCrw.nextLine();
		int entry = 0;
		PreparedStatement direct = con.prepareStatement(INSERT_DIRECTOR);
		PreparedStatement writr = con.prepareStatement(INSERT_WRITER);
		while (scCrw.hasNextLine()) {

			String line = scCrw.nextLine();
			String[] splitLine = line.split(TAB);
			int mid = Integer.parseInt(splitLine[0].replace(TITLE, ""));
			direct.setInt(1, mid);
			writr.setInt(1, mid);

			if (splitLine[1].contains(",")) {
				String[] splitDir = splitLine[1].split(",");
				for (String director : splitDir) {
					entry++;
					if (!director.equals(NULL_STR)) {
						direct.setInt(2, Integer.parseInt(director.replace(NAME, "")));
						direct.addBatch();
					}
				}
			} else {
				
				if (!splitLine[1].equals(NULL_STR)) {
					entry++;
					direct.setInt(2, Integer.parseInt(splitLine[1].replace(NAME, "")));
					direct.addBatch();
				}
			}

			if (splitLine[2].contains(",")) {
				String[] splitWrt = splitLine[2].split(",");
				for (String writer : splitWrt) {
					entry++;
					if (!writer.equals(NULL_STR)) {
						writr.setInt(2, Integer.parseInt(writer.replace(NAME, "")));
						writr.addBatch();
					}
				}
			}else {
				if (!splitLine[2].equals(NULL_STR)) {
					entry++;
					writr.setInt(2, Integer.parseInt(splitLine[2].replace(NAME, "")));
					writr.addBatch();
				}
			}

			if (entry % batchSize == 0) {
				direct.executeBatch();
				writr.executeBatch();
				con.commit();
			}
		}
		direct.executeBatch();
		writr.executeBatch();
		con.commit();

		direct.close();
		writr.close();
		gzipStreamCrew.close();
		scCrw.close();
	}

	private static void deleteADPW(Connection con) throws Exception {
		PreparedStatement act = con.prepareStatement(DELETE_FROM_ACTOR_M);
		act.execute();
		con.commit();
		act.close();

		act = con.prepareStatement(DELETE_FROM_ACTOR_P);
		act.execute();
		con.commit();
		act.close();
		
		PreparedStatement prod = con.prepareStatement(DELETE_FROM_PRODUCER_M);
		prod.execute();
		con.commit();
		prod.close();
		
		prod = con.prepareStatement(DELETE_FROM_PRODUCER_P);
		prod.execute();
		con.commit();
		prod.close();

		PreparedStatement writr = con.prepareStatement(DELETE_FROM_WRITER_M);
		writr.execute();
		con.commit();
		writr.close();
		
		writr = con.prepareStatement(DELETE_FROM_WRITER_P);
		writr.execute();
		con.commit();
		writr.close();

		PreparedStatement direct = con.prepareStatement(DELETE_FROM_DIRECTOR_M);
		direct.execute();
		con.commit();
		direct.close();
		
		direct = con.prepareStatement(DELETE_FROM_DIRECTOR_P);
		direct.execute();
		con.commit();
		direct.close();
	}

	private static void dropALL(Connection con) throws Exception {
		PreparedStatement st = con.prepareStatement(DROP_ALL);
		st.execute();
		con.commit();
		st.close();
	}
}

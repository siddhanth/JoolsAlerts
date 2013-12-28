package com.jools.mongo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Hello world!
 * 
 */
public class App {

	private static String tester;
	private static String os;
	private static String kingdom;
	private static String phylum;
	private static String class_v;
	private static String family;
	private static String genus;
	private static String species;
	private static String order;
	private static String browser;
	private static String server;
	private static int port;
	private static String db;
	private static String user;
	private static char[] password;
	private static String collection;
	private static String startTime;
	private static String outFile;
	private static String configFile;

	String[] header = new String[] { "userid", "date", "count", "os",
			"browser", "tester", "kingdom", "phylum", "class", "family",
			"genus", "species", "order" };

	public void executeQuery(DBCollection collection, DBObject ref)
			throws IOException {
		DBCursor cursor = collection.find(ref);
		BufferedWriter bw = new BufferedWriter(new FileWriter(outFile));
		for (String key : header) {
			bw.write(key + "\t");
		}
		bw.write("\n");
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			for (String key : header) {
				if (obj.containsField(key)) {
					bw.write(obj.get(key).toString());
				}
				bw.write("\t");
			}
			bw.write("\n");
		}
		bw.close();
	}

	public static void main(String[] args) throws IOException {
		BasicDBObject query = new BasicDBObject();
		for (int ix = 0; ix < args.length; ix++) {
			if (args[ix].equals("-b")) {
				browser = args[ix + 1];
				query.append("browser", browser);
			} else if (args[ix].equals("-o")) {
				os = args[ix + 1];
				query.append("os", os);
			} else if (args[ix].equals("-t")) {
				tester = args[ix + 1];
				query.append("tester", tester);
			} else if (args[ix].equals("-k")) {
				kingdom = args[ix + 1];
				query.append("kingdom", kingdom);
			} else if (args[ix].equals("-p")) {
				phylum = args[ix + 1];
				query.append("phylum", phylum);
			} else if (args[ix].equals("-c")) {
				class_v = args[ix + 1];
				query.append("class", class_v);
			} else if (args[ix].equals("-f")) {
				family = args[ix + 1];
				query.append("family", family);
			} else if (args[ix].equals("-g")) {
				genus = args[ix + 1];
				query.append("genus", genus);
			} else if (args[ix].equals("-s")) {
				species = args[ix + 1];
				query.append("species", species);
			} else if (args[ix].equals("-or")) {
				order = args[ix + 1];
				query.append("order", order);
			} else if (args[ix].equals("-date")) {
				startTime = args[ix + 1];
				query.append("date", Integer.parseInt(startTime));
			} else if (args[ix].equals("-out")) {
				outFile = args[ix + 1];
			} else if (args[ix].equals("-config")) {
				configFile = args[ix + 1];
			}

		}
		if (outFile == null) {
			System.err
					.println("usage: [-b <browser> -o <os> -t <tester> -k <kingdom> -c"
							+ " <class> -f <family> -g <genus> -s "
							+ "<species> -or <order>] -date <date> -out <outfile>");
			System.exit(0);
		}
		BufferedReader br = new BufferedReader(new FileReader(configFile));
		String line;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split(":");
			if (parts[0].equals("server")) {
				server = parts[1];
			} else if (parts[0].equals("port")) {
				port = Integer.parseInt(parts[1]);
			} else if (parts[0].equals("db")) {
				db = parts[1];
			} else if (parts[0].equals("user")) {
				user = parts[1];
			} else if (parts[0].equals("password")) {
				password = parts[1].toCharArray();
			}
		}
		br.close();
		MongoClient mongoClient = new MongoClient(server, port);
		DB mongoDB = mongoClient.getDB(db);
		boolean auth = mongoDB.authenticate(user, password);
		if (!auth) {
			System.err.println("Authentication Failed");
		}

		DBCollection coll = mongoDB.getCollection("counters");
		App obj = new App();

		obj.executeQuery(coll, query);
		System.out.println("Done");
	}
}

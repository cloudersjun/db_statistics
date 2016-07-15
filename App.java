package db.statistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hello world!
 *
 */
public class App {

	public static class NewThread implements Runnable {
		Thread t;
		int dbno;
		int dbgroupid;

		NewThread() {
			t = new Thread(this, "Demo Thread");
			System.out.println("Child thread: " + t);
			t.start();
		}

		NewThread(int dbno, int dbgroupid) {
			this.dbno = dbno;
			this.dbgroupid = dbgroupid;
			t = new Thread(this);
			//System.out.println("Child thread: " + t);
			t.start();
		}

		public void run() {
			try {
				db_statistics(dbno, dbgroupid);
				//System.out.println(dbno +":"+ dbgroupid);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println(e);
			}

		}
	}

	static int db[][] = { { 2, 3, 4, 5, 7 }, { 2, 1, 3, 4, 5 } };

	public static String driver = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
		App app = new App();
		for (int i = 0; i < db[0].length; i++) {
			System.out.println("db" + db[0][i] + " begin: ");
			NewThread thread = new NewThread(db[0][i], db[1][i]);
			//thread.run();
		}
		System.out.println("Hello World!");
	}

	public static void db_statistics(int dbno, int dbgroupid) {
		String url = "jdbc:mysql://l-db" + dbno + "-5.prod.cn2.corp.agrant.cn/";// db
		String db1url = "jdbc:mysql://l-db1-5.prod.cn2.corp.agrant.cn/";
		String filename = "E:\\DB" + dbno + ".txt";
		String user = "dev";
		String password = "vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw";
		Connection conn = null;
		Connection conn2 = null;
		File file = new File(filename);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(file);

			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(db1url, user, password);
		} catch (FileNotFoundException | InstantiationException | IllegalAccessException | ClassNotFoundException
				| SQLException e1) {
			e1.printStackTrace();
		}
		String dbsql = "SELECT DISTINCT T.cid FROM AGUsers.CidDBGroup D LEFT JOIN Sched.Task T" + " ON D.cid = T.cid "
				+ " WHERE T.groupid in (11,41,23,63)" + " AND T.targettime >='2016-07-12' AND targettime < '2016-07-13'"
				+ " AND T.trkey != 'systrig'" + " AND D.dbgroupid = " + dbgroupid + ";";
		Statement statement = null;
		Statement statement2 = null;
		ResultSet rs = null;
		ResultSet rs2 = null;
		ResultSet rs_kwcount = null;
		int cid = -1;
		double count = -1;
		String size = "-1";
		String oldsize = "-1";
		if (conn != null) {

			try {
				statement = conn.createStatement();
				rs = statement.executeQuery(dbsql);
				String head = "cid\tsize\toldsize\tkwcount\t\n";
				out.write(head.getBytes());
				while (rs.next()) {

					cid = rs.getInt("cid");
					conn2 = DriverManager.getConnection(url, user, password);
					String sql = "SELECT (SUM(DATA_LENGTH)+SUM(INDEX_LENGTH))/1048576 AS S1 FROM information_schema.tables  T WHERE T.TABLE_SCHEMA='AD_BASE_"
							+ cid + "';";
					// System.out.println(sql);
					if (conn2 != null) {
						statement2 = conn2.createStatement();
						try {
							rs2 = statement2.executeQuery(sql);
						} catch (SQLException e2) {

						}
						while (rs2.next()) {
							size = rs2.getString(1);
						}
						sql = "SELECT (SUM(DATA_LENGTH)+SUM(INDEX_LENGTH))/1048576 AS S1 FROM information_schema.tables  T WHERE T.TABLE_SCHEMA='AD_BASE_"
								+ cid + "'"+ " AND TABLE_NAME LIKE  'OLD%';";
						
						try {
							rs2 = statement2.executeQuery(sql);
						} catch (SQLException e2) {

						}
						
						while (rs2.next()) {
							oldsize = rs2.getString(1);
						}
						
						sql = "SELECT count(*) AS count FROM AD_BASE_" + cid
								+ ".`AD3` WHERE updatetime >= '2016-07-12';";
						
						try {
							rs_kwcount = conn2.createStatement().executeQuery(sql);
						} catch (SQLException e1) {
							continue;
						}
						while (rs_kwcount.next()) {
							count = rs_kwcount.getDouble(1);
							String s = cid + "\t" + size + "\t" +oldsize+"\t"+ count + "\t" + "\n";
							out.write(s.getBytes());
							//System.out.println(cid + "\t" + size + "\t" + count + "\t");
						}
					}
				}
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("db"+dbno+"  over!");
	}

}

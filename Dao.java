/**
 * 
 */
package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 2016年8月3日
 *
 * @author cloud
 *
 */
public class Dao {

	private final String userName = "dev";

	private final String passwd = "";

	private final String driver = "com.mysql.jdbc.Driver";

	private static class DaoHelper {

		private static Dao factory = new Dao();

	}

	private Dao() {
		try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static final Dao getDao() {
		return DaoHelper.factory;
	}

	private volatile static Connection dbConn1 = null;

	private volatile static Connection dbConn2 = null;

	private volatile static Connection dbConn3 = null;

	private volatile static Connection dbConn4 = null;

	private volatile static Connection dbConn5 = null;

	private volatile static Connection dbConn7 = null;

	public synchronized Connection getDbConn(int dbId, int cid) {
		Connection conn = null;
		String url = "";
		try {
			Class.forName(driver).newInstance();
			url = "";
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		switch (dbId) {
		case 2:
			if (dbConn2 == null) {
					try {
						dbConn2 = DriverManager.getConnection(url, userName, passwd);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				System.out.println("db2 connection create!");
			}
			conn = dbConn2;
			break;
		case 3:
			if (dbConn3 == null) {
					try {
						dbConn3 = DriverManager.getConnection(url, userName, passwd);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				System.out.println("db3 connection create!");
			}
			conn = dbConn3;
			break;
		case 4:
			if (dbConn4 == null) {
					try {
						dbConn4 = DriverManager.getConnection(url, userName, passwd);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				System.out.println("db4 connection create!");
			}
			conn = dbConn4;
			break;
		case 5:
			if (dbConn5 == null) {
					try {
						dbConn5 = DriverManager.getConnection(url, userName, passwd);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				System.out.println("db5 connection create!");
			}
			conn = dbConn5;
			break;
		case 7:
			if (dbConn7 == null) {
					try {
						dbConn7 = DriverManager.getConnection(url, userName, passwd);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				System.out.println("db7 connection create!");
			}
			conn = dbConn7;
			break;
		default:
			break;
		}
		return conn;
	}

	public synchronized void close(int dbId) {
		try {
			switch (dbId) {
			case 1:
				dbConn1.close();
				System.out.println(dbId + " closed");
				break;
			case 2:
				dbConn2.close();
				System.out.println(dbId + " closed");
				break;
			case 3:
				dbConn3.close();
				System.out.println(dbId + " closed");
				break;
			case 4:
				dbConn4.close();
				System.out.println(dbId + " closed");
				break;
			case 5:
				dbConn5.close();
				System.out.println(dbId + " closed");
				break;
			case 7:
				dbConn7.close();
				System.out.println(dbId + " closed");
				break;
			default:
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public synchronized Connection getDbConn() {
		if (dbConn1 == null) {
			System.out.println("db1 connection :");
				try {
					String url = "";
					dbConn1 = DriverManager.getConnection(url, userName, passwd);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			System.out.println("db1 connection create!");
		}
		return dbConn1;
	}

}

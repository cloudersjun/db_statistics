package db.statistics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import dao.CidListDao;
import dao.Dao;
import dao.SizeDao;

/**
 * Hello world!
 *
 */
public class App {

	public static class NewThread implements Runnable {
		Thread t;
		int dbno;
		int dbgroupid;
		String starttime;
		String endtime;

		NewThread() {
			t = new Thread(this, "Demo Thread");
			System.out.println("Child thread: " + t);
			t.start();
		}

		NewThread(int dbno, int dbgroupid, String starttime, String endtime) {
			this.dbno = dbno;
			this.dbgroupid = dbgroupid;
			this.starttime = starttime;
			this.endtime = endtime;
			t = new Thread(this);
			// System.out.println("Child thread: " + t);
			t.start();
		}

		public void run() {
			try {
				db_statistics(dbno, dbgroupid, starttime, endtime);
				Dao.getDao().close(1);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println(e);
			}

		}
	}

	static int db[][] = { { 2, 3, 4, 5, 7 }, { 2, 1, 3, 4, 5 } };

	// static int db[][] = { { 2, 7 }, { 2, 5 } };

	public static String driver = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
		for (int i = 0; i < db[0].length; i++) {
			System.out.println("db" + db[0][i] + " begin: ");
			new NewThread(db[0][i], db[1][i], args[0], args[1]);
			// thread.run();

		}
		System.out.println(new Date().toString());
	}

	public static void db_statistics(int dbno, int dbgroupid, String starttime, String endtime) {
		List<Integer> cids = new ArrayList<>();
		CidListDao cidListDao = new CidListDao();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		cids = cidListDao.getAllCidByStartTimeEndTimeDbGroupid(starttime, endtime, dbgroupid);
//		Dao.getDao().close(dbno);
		SizeDao sizeDao = new SizeDao();
		String directory = "E:\\" + starttime;
		String filename = directory + "\\DB" + dbno + ".txt";
		File file = new File(filename);
		File dir = new File(directory);
		FileOutputStream out = null;
		try {
			if (!dir.exists()) {
				dir.mkdirs();
			}
			file.createNewFile();
			out = new FileOutputStream(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		double count = -1;
		String size = "-1";
		String oldsize = "-1";
		String head = "cid\tsize\toldsize\tkwcount\n";
		try {
			out.write(head.getBytes());
		} catch (IOException e3) {
			e3.printStackTrace();
		}
		for (Integer cid : cids) {
			try {
				size = sizeDao.getSizeByCid(dbno, cid);
				oldsize = sizeDao.getOldSizeByCid(dbno, cid);
				count = sizeDao.getKwCountByCidStartTime(dbno, cid, starttime);
				String s = cid + "\t" + size + "\t" + oldsize + "\t" + count + "\n";
				out.write(s.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("db" + dbno + "  over!");
		Dao.getDao().close(dbno);
		System.out.println(new Date().toString());
	}

}

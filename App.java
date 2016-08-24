package db.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

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
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println(e);
			}
			Dao.getDao().close(1);
		}
	}

	static int db[][] = { { 2, 3, 4, 5, 7 }, { 2, 1, 3, 4, 5 } };

	// static int db[][] = { { 7 }, { 5 } };

	public static String driver = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
		for (int i = 0; i < db[0].length; i++) {
			System.out.println("db" + db[0][i] + " begin: ");
			String starttime = "";
			String endtime = "";
			// System.out.println(args.length);
			if (args.length == 0) {
				starttime = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
				endtime = DateFormatUtils.format(DateUtils.addDays(new Date(), +1), "yyyy-MM-dd");
			} else if (args.length == 2) {
				starttime = args[0];
				endtime = args[1];
			}
			// System.out.println(starttime + " " + endtime);
			new NewThread(db[0][i], db[1][i], starttime, endtime);
			// thread.run();
		}
		System.out.println(new Date().toString());
	}

	public static void db_statistics(int dbno, int dbgroupid, String starttime, String endtime) {
		List<Integer> cids = new ArrayList<>();
		CidListDao cidListDao = new CidListDao();
		cids = cidListDao.getAllCidByStartTimeEndTimeDbGroupid(starttime, endtime, dbgroupid);
		System.out.println("db:" + dbgroupid + "\tcid size:" + cids.size());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		String directory = "E:\\" + starttime;
		String filename = directory + "\\DB" + dbno + ".txt";
		// String tempname = directory + "\\DB" + dbno + ".temp";
		File file = new File(filename);
		File dir = new File(directory);
		FileOutputStream out = null;
		BufferedReader bf = null;
		String head = "cid\tsize\tkwcount\n";
		String result = head;
		List<Integer> alreadyCids = new ArrayList<>();
		try {
			if (!dir.exists()) {
				dir.mkdirs();
			}
			if (file.exists()) {
				bf = new BufferedReader(new FileReader(file));
			} else {
				file.createNewFile();
			}
			out = new FileOutputStream(file, true);
			if (bf != null) {
				String line = "";
				while ((line = bf.readLine()) != null) {
					String[] lines = line.split("\t");
					if (lines.length == 3 && !lines[2].equals("null")) {
						if (!lines[0].equals("cid")) {
							int cid = Integer.valueOf(lines[0]);
							System.out.println("don't statistic cid: " + cid);
							alreadyCids.add(cid);
						}
					} else {
						System.out.print(line);
						result += line;
					}
				}
			} else {
				out.write(head.getBytes());
			}
			cids.removeAll(alreadyCids);
			double count = -1;
			String size = "-1";
			// String oldsize = "-1";
			SizeDao sizeDao = new SizeDao();
			for (Integer cid : cids) {
				size = sizeDao.getSizeByCid(dbno, cid);
				// oldsize = sizeDao.getOldSizeByCid(dbno, cid);
				count = sizeDao.getKwCountByCidStartTime(dbno, cid, starttime);
				String s = cid + "\t" + size + "\t" + count + "\n";
				out.write(s.getBytes());
			}
			out.close();
			result += getResultByCids(cids, dbno, starttime);
			out = new FileOutputStream(file);
			out.write(result.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (bf != null) {
			try {
				bf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("db" + dbno + "  over!");
		Dao.getDao().close(dbno);
		System.out.println(new Date().toString());
	}

	private static String getResultByCids(List<Integer> cids, int dbno, String starttime) {
		String ret = "";
		double count = -1;
		String size = "-1";
		// String oldsize = "-1";
		SizeDao sizeDao = new SizeDao();
		for (Integer cid : cids) {
			size = sizeDao.getSizeByCid(dbno, cid);
			// oldsize = sizeDao.getOldSizeByCid(dbno, cid);
			count = sizeDao.getKwCountByCidStartTime(dbno, cid, starttime);
			String s = cid + "\t" + size + "\t" + count + "\n";
			ret += s;
		}
		return ret;
	}

}

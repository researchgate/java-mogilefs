package com.guba.mogilefs.test;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guba.mogilefs.LocalFileMogileFSImpl;
import com.guba.mogilefs.MogileFS;

public class TestStore {

	private static final Logger log = LoggerFactory.getLogger(TestStore.class);

	@Test
	public void testStoreALot() {
		try {
			MogileFS mfs = new LocalFileMogileFSImpl(new File("."), "emloffice.guba.com");

			File file = new File("java/com/guba/mogilefs/PooledMogileFSImpl.java");
			int count = 2;

			for (int i = 0; i < count; i++) {
				Thread thread = new Thread(new StoreSomething(mfs, file));
				thread.start();
			}


		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}


	private static class StoreSomething implements Runnable {

		private static final Logger log = LoggerFactory.getLogger(StoreSomething.class);

		private MogileFS mfs;
		private File file;

		public StoreSomething(final MogileFS mfs, final File file) {
			this.mfs = mfs;
			this.file = file;
		}

		public void run() {
			try {
				String key = Double.toString(Math.random());

				log.info("starting store of " + key);
				mfs.storeFile(key, "derived", file);
				log.info("ending store of " + key);

				log.info("starting delete of " + key);
				//mfs.delete(key);
				log.info("ending delete of " + key);

			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		}

	}

}

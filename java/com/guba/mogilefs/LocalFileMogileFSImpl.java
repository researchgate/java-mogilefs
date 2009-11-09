package com.guba.mogilefs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

public class LocalFileMogileFSImpl implements MogileFS {

	private Logger log = Logger.getLogger(LocalFileMogileFSImpl.class);

	private File topDir;

	private String domain;
	private File domainDir;

	public LocalFileMogileFSImpl(final File topDir, final String domain) throws IOException {
		this.topDir = topDir;
		this.domain = domain;

		if (!topDir.exists()) {
			throw new FileNotFoundException(topDir.getAbsolutePath());
		}

		this.domainDir = new File(topDir, domain);
		if (!domainDir.exists()) {
			if (!domainDir.mkdir()) {
				throw new FileNotFoundException(domainDir.getAbsolutePath());
			}
		}
	}

	public void reload(final String domain, final String[] trackerStrings)
	throws NoTrackersException, BadHostFormatException {
		this.domain = domain;
		this.domainDir = new File(topDir, domain);
		if (!domainDir.exists()) {
			if (!domainDir.mkdir()) {
				log.error("couldn't make dir " + domainDir.getAbsolutePath());
				throw new NoTrackersException();
			}
		}
	}

	public OutputStream newFile(final String key, final String storageClass,
			final long byteCount) throws NoTrackersException,
			TrackerCommunicationException, StorageCommunicationException {

		File file = new File(domainDir, key);
		try {
			return new FileOutputStream(file);

		} catch (IOException e) {
			throw new StorageCommunicationException("couldn't open file " + file.getAbsolutePath());
		}
	}


	public void storeFile(final String key, final String storageClass, final File file)
	throws MogileException {
		File storedFile = new File(domainDir, key);

		try {
			FileOutputStream out = new FileOutputStream(storedFile);
			FileInputStream in = new FileInputStream(file);

			byte[] buffer = new byte[1024];
			int count = 0;
			while ((count = in.read(buffer)) >= 0) {
				out.write(buffer, 0, count);
			}

			out.close();
			in.close();

		} catch (IOException e) {

			throw new StorageCommunicationException(e.getMessage());
		}

	}

	public File getFile(final String key, final File destination)
	throws NoTrackersException, TrackerCommunicationException,
	IOException, StorageCommunicationException {
		File storedFile = new File(domainDir, key);

		try {
			FileOutputStream out = new FileOutputStream(destination);
			FileInputStream in = new FileInputStream(storedFile);

			byte[] buffer = new byte[1024];
			int count = 0;
			while ((count = in.read(buffer)) >= 0) {
				out.write(buffer, 0, count);
			}

			out.close();
			in.close();

			return destination;

		} catch (IOException e) {

			throw new StorageCommunicationException(e.getMessage());
		}
	}

	public byte[] getFileBytes(final String key) throws NoTrackersException,
	TrackerCommunicationException, IOException,
	StorageCommunicationException {
		File storedFile = new File(domainDir, key);

		byte[] buffer = new byte[(int) storedFile.length()];
		int offset = 0;
		int count = 0;

		FileInputStream in = new FileInputStream(storedFile);
		while ((count = in.read(buffer, offset, buffer.length - offset)) >= 0) {
			offset += count;
		}

		return buffer;
	}

	public InputStream getFileStream(final String key) throws NoTrackersException,
	TrackerCommunicationException, StorageCommunicationException {
		// TODO Auto-generated method stub
		File storedFile = new File(domainDir, key);

		try {
			return new FileInputStream(storedFile);
		} catch (IOException e) {
			throw new StorageCommunicationException(e.getMessage());
		}
	}

	public void delete(final String key) throws NoTrackersException,
	NoTrackersException {
		File storedFile = new File(domainDir, key);

		storedFile.delete();
	}

	public void sleep(final int seconds) throws NoTrackersException,
	TrackerCommunicationException {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			// ignore
		}
	}

	public Object[] listKeys(final String key) throws NoTrackersException {
		throw new UnsupportedOperationException("Not supported.");
	}

	public Object[] listKeys(final String key, final int limit) throws NoTrackersException {
		throw new UnsupportedOperationException("Not supported.");
	}

	/**
	 * Return a list of keys matching your key. Return
	 * null if there was an error from the server.
	 * 
	 * @param key
	 * @return array of Strings that are keys
	 * @throws NoTrackersException
	 */
	public Object[] listKeys(final String key, final String after, final int limit) throws NoTrackersException {
		throw new UnsupportedOperationException("Not supported.");
	}


	public void rename(final String fromKey, final String toKey) throws NoTrackersException {
		File fromFile = new File(domainDir, fromKey);
		File toFile = new File(domainDir, toKey);

		fromFile.renameTo(toFile);
	}

	public String[] getPaths(final String key, final boolean noverify)
	throws NoTrackersException {
		File storedFile = new File(domainDir, key);

		return new String[] { "file://" + storedFile.getAbsolutePath() };
	}

	public String getDomain() {
		return domain;
	}

}

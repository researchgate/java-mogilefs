/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;

import org.junit.Test;

import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;

/**
 * @author ericlambrecht
 * 
 */
public class TestMogileFS {

	@Test
	public void testMogileFS() {
		try {
			MogileFS mfs = new PooledMogileFSImpl("www.guba.com",
					new String[] { "qbert.guba.com:7001" }, 0, 1, 10000);

			File file = new File("java/com/guba/mogilefs/PooledMogileFSImpl.java");
			if (file.exists()) {
				OutputStream out = mfs.newFile("PooledMogileFSImpl.java",
						"oneDeviceTest", file.length());
				FileInputStream in = new FileInputStream(file);
				byte[] buffer = new byte[1024];
				int count = 0;
				while ((count = in.read(buffer)) >= 0) {
					out.write(buffer, 0, count);
				}
				in.close();
				out.close();
			}

			//log.debug("success!");

		} catch (Exception e) {
			//log.error("top level exception", e);
		}
	}

}

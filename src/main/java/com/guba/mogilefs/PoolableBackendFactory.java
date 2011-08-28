package com.guba.mogilefs;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.pool.PoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolableBackendFactory implements PoolableObjectFactory {

	private static final Logger log = LoggerFactory.getLogger(PoolableObjectFactory.class);

	private List<InetSocketAddress> trackers;

	public PoolableBackendFactory(final List<InetSocketAddress> trackers) {
		log.debug("new backend factory created");

		this.trackers = trackers;
	}

	public Object makeObject() throws Exception {
		try {
			Backend backend = new Backend(trackers, true);

			if (log.isDebugEnabled()) {
				log.debug("making object " + backend.toString());
			}

			return backend;
		} catch (Exception e) {
			log.debug("problem making backend", e);

			throw e;
		}
	}

	public void destroyObject(final Object obj) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("destroying object '" + obj.toString() + "'");
		}

		if (obj instanceof Backend) {
			Backend backend = (Backend) obj;
			backend.destroy();
		}
	}

	public boolean validateObject(final Object obj) {
		if (obj instanceof Backend) {
			Backend backend = (Backend) obj;
			boolean connected = backend.isConnected();

			if (log.isDebugEnabled()) {
				if (!connected) {
					log.debug("validating " + obj.toString() + ". Not valid! Last err was: " + backend.getLastErr());
				} else {
					log.debug("validating " + obj.toString() + ". validated");
				}
			}

			return connected;
		}

		log.debug("validating non-Backend object");
		return false;
	}

	public void activateObject(final Object arg0) throws Exception {
		// nothing to do
		if (log.isDebugEnabled()) {
			log.debug("activating object " + arg0.toString());
		}
	}

	public void passivateObject(final Object arg0) throws Exception {
		// nothing to do
		if (log.isDebugEnabled()) {
			log.debug("passivating object" + arg0.toString());
		}
	}

}

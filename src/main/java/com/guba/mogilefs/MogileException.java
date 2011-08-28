/*
 * Created on Jun 15, 2005
 *
 * 
 */
package com.guba.mogilefs;

/**
 * The various PooledMogileFSImpl related exceptions are all subclassed from this to
 * easily catch them.
 * 
 * @author eml@guba.com
 */
public class MogileException extends Exception {

	private static final long serialVersionUID = -6737817547677933860L;

	/**
	 * 
	 */
	public MogileException() {
		super();
	}

	/**
	 * @param message
	 */
	public MogileException(final String message) {
		super(message);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MogileException(final String message, final Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param cause
	 */
	public MogileException(final Throwable cause) {
		super(cause);
	}

}

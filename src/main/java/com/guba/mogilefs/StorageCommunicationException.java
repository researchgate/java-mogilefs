/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs;

/**
 * @author ericlambrecht
 * 
 */
public class StorageCommunicationException extends MogileException {

	private static final long serialVersionUID = 6557930929763915323L;

	public StorageCommunicationException(final String message) {
		super(message);
	}

	public StorageCommunicationException(final String message, final Throwable t) {
		super(message, t);
	}

}

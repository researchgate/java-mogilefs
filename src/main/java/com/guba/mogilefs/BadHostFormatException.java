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
public class BadHostFormatException extends MogileException {

	private static final long serialVersionUID = 1L;

	public BadHostFormatException(final String host) {
		super(host);
	}

}

/**
 * Copyright (C) 2004 - 2011 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */
package com.guba.mogilefs;

import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author vigumnov
 * @since 8/30/11
 */
public class MogileStreamEntity extends AbstractHttpEntity {
    private final InputStream content;
    private final long length;
    private long bytesSent = 0;
    private int bufferSize = 2048;

    public MogileStreamEntity(final InputStream instream) {
        this(instream, -1);
    }

    public MogileStreamEntity(final InputStream instream, long length) {
        super();
        if (instream == null) {
            throw new IllegalArgumentException("Source input stream may not be null");
        }
        this.content = instream;
        this.length = length;
        this.setContentType("binary/octet-stream");
        if (length < 0) {
            this.setChunked(true);
        }
    }

    public boolean isRepeatable() {
        return false;
    }

    public long getContentLength() {
        return this.length;
    }

    public InputStream getContent() throws IOException {
        return this.content;
    }

    public void writeTo(final OutputStream outstream) throws IOException {
        if (outstream == null) {
            throw new IllegalArgumentException("Output stream may not be null");
        }
        InputStream instream = this.content;
        byte[] buffer = new byte[bufferSize];
        int l;
        if (this.length < 0) {
            // consume until EOF
            while ((l = instream.read(buffer)) != -1) {
                bytesSent += l;
                outstream.write(buffer, 0, l);
            }
        } else {
            // consume no more than length
            long remaining = this.length;
            while (remaining > 0) {
                l = instream.read(buffer, 0, (int) Math.min(bufferSize, remaining));
                if (l == -1) {
                    break;
                }
                outstream.write(buffer, 0, l);
                bytesSent += l;
                remaining -= l;
            }
        }
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public boolean isStreaming() {
        return true;
    }
}

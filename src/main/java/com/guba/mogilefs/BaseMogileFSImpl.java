package com.guba.mogilefs;

import org.apache.commons.pool.ObjectPool;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.ContentEncodingHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseMogileFSImpl implements MogileFS {

    private static final Logger log = LoggerFactory.getLogger(BaseMogileFSImpl.class);

    private String domain;

    protected List<InetSocketAddress> trackers;

    private ObjectPool cachedBackendPool;

    private int maxRetries = 2;
    private int retrySleepTime = 2000;

    /* Should we preserve the order of paths that we get from the server. */
    private boolean keepPathOrder;

    public BaseMogileFSImpl(final String domain, final String[] trackerStrings)
            throws BadHostFormatException, NoTrackersException {
        this(domain, trackerStrings, false);
    }

    public BaseMogileFSImpl(final String domain, final String[] trackerStrings, final boolean shouldKeepPathOrder)
            throws BadHostFormatException, NoTrackersException {
        keepPathOrder = shouldKeepPathOrder;
        trackers = parseHosts(trackerStrings);

        reload(domain);
    }

    /**
     * Parse the host strings to InetSocketAddress objects. If something can't
     * be parsed, an exception is thrown.
     *
     * @param hostStrings array of host:port string pairs
     * @return a List of InetSocketAddress objects.
     * @throws BadHostFormatException if one of the strings isn't in the right format.
     */
    protected List<InetSocketAddress> parseHosts(final String[] hostStrings) throws BadHostFormatException {
        if (hostStrings == null) {
            return Collections.emptyList();
        }
        List<InetSocketAddress> list = new ArrayList<InetSocketAddress>(hostStrings.length);
        Pattern hostAndPortPattern = Pattern.compile("^(\\S+):(\\d+)$");

        for (int i = 0; i < hostStrings.length; i++) {
            final String hostString = hostStrings[i];
            Matcher m = hostAndPortPattern.matcher(hostString);
            if (!m.matches()) {
                throw new BadHostFormatException(hostString);
            }

            if (log.isDebugEnabled()) {
                log.debug("parsed tracker " + hostString);
            }

            InetSocketAddress addr = new InetSocketAddress(m.group(1),
                    Integer.parseInt(m.group(2)));
            list.add(addr);
        }

        return list;
    }

    /**
     * Set things up again.
     *
     * @param domain
     * @param trackerStrings
     * @throws NoTrackersException
     */
    public void reload(final String domain, final String trackerStrings[])
            throws NoTrackersException, BadHostFormatException {
        this.trackers = parseHosts(trackerStrings);

        reload(domain);
    }

    /**
     * This is called whenever we get a new domain or set of trackers to connect to.
     */

    protected void reload(final String domain) throws NoTrackersException {
        this.domain = domain;

        // make sure this is rebuilt when we next request it
        cachedBackendPool = null;
    }

    protected abstract ObjectPool buildBackendPool();

    protected ObjectPool getBackendPool() {
        if (cachedBackendPool != null) {
            return cachedBackendPool;
        }

        cachedBackendPool = buildBackendPool();

        return cachedBackendPool;
    }

    /**
     * Get a ref to an OutputStream so you can store a new file. The original
     * implementation returns null on an error, but this one throws an exception
     * if something goes wrong, so null is never returned.
     *
     * @param key          name to give the new file
     * @param storageClass the class to store it under
     * @param byteCount    the size of the new file (needed for content-length header
     * @return non-null stream you should write byteCount bytes to
     */
    public OutputStream newFile(final String key, final String storageClass, final long byteCount)
            throws NoTrackersException, TrackerCommunicationException, StorageCommunicationException {
        Backend backend = null;

        try {
            // get a backend
            backend = borrowBackend();

            Map<String, String> response = backend.doRequest("create_open", new String[]{
                    "domain", domain, "class", storageClass, "key", key});

            if (response == null) {
                throw new TrackerCommunicationException(backend.getLastErr() + ", " + backend.getLastErrStr());
            }

            if ((response.get("path") == null) || (response.get("fid") == null)) {
                throw new TrackerCommunicationException("create_open response from tracker " + backend.getTracker() +
                        " missing fid or path (err:" + backend.getLastErr() + ", " + backend.getLastErrStr() + ")");
            }

            try {
                return new MogileOutputStream(getBackendPool(), domain, response.get("fid"), response.get("path"),
                        response.get("devid"), key,
                        byteCount);

            } catch (MalformedURLException e) {
                // hrmm.. this shouldn't happen - we'll blame it on the tracker
                log.warn("error trying to store file with malformed url: " + response.get("path"));
                throw new TrackerCommunicationException(
                        "error trying to store file with malformed url: " + response.get("path"));

            }

        } catch (TrackerCommunicationException e) {
            // lets nuke this backend connection and make a new one
            if (backend != null) {
                invalidateBackend(backend);
                backend = null;
            }

            throw e;

        } finally {
            // make sure to return the backend to the pool
            if (backend != null) {
                returnBackend(backend);
            }
        }
    }


    /**
     * Set the max number of times to try retry storing a file with 'storeFile' or
     * deleting a file with 'delete'. If this is -1, then never stop retrying. This value
     * defaults to -1.
     *
     * @param maxRetries
     */
    public void setMaxRetries(final int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * After a failed 'storeFile' request, sleep for this number of milliseconds before
     * retrying the store. Defaults to 2 seconds.
     *
     * @param retrySleepTime
     */
    public void setRetryTimeout(final int retrySleepTime) {
        this.retrySleepTime = retrySleepTime;
    }


    public void storeStream(final String key, final String storageClass, final InputStream is)
            throws MogileException {
        // chunked
        storeStream(key, storageClass, is, -1);
    }

    public void storeStream(final String key, final String storageClass, final InputStream is, final long fileSize)
            throws MogileException {
        int attempt = 1;

        Backend backend = null;

        while ((maxRetries == -1) || (attempt++ <= maxRetries)) {
            try {
                backend = borrowBackend();

                Map<String, String> response = backend.doRequest("create_open", new String[]{
                        "domain", domain, "class", storageClass, "key", key});

                if (response == null) {
                    log.warn("problem talking to backend: " + backend.getLastErrStr() + " (err: "
                            + backend.getLastErr() + ")");

                } else {
                    try {
                        String path = response.get("path");
                        String fid = response.get("fid");
                        String devid = response.get("devid");

                        HttpClient client = new ContentEncodingHttpClient();
                        HttpPut putReq = new HttpPut(path);
                        InputStreamEntity ent = new InputStreamEntity(is, fileSize);
                        ent.setContentType("binary/octet-stream");
                        if (fileSize < 0) {
                            ent.setChunked(true);
                        }
                        putReq.setEntity(ent);
                        client.execute(putReq);

                        is.close();

                        Map<String, String> closeResponse = backend.doRequest("create_close", new String[]{
                                "fid", fid, "devid", devid, "domain", domain, "size",
                                Long.toString(ent.getContentLength()), "key", key, "path", path});

                        if (closeResponse == null) {
                            throw new IOException(backend.getLastErrStr());
                        }

                        // success!
                        return;

                    } catch (MalformedURLException e) {
                        // hrmm.. this shouldn't happen - we'll blame it on the tracker
                        log.warn("error trying to retrieve file with malformed url: " + response.get("path"));

                    } catch (IOException e) {
                        log.warn("error trying to store file", e);
                    }

                }

            } catch (Exception e) {
                log.warn("problem trying to store file on mogile", e);

                // something went wrong - get rid of the Backend object
                if (backend != null) {
                    invalidateBackend(backend);
                    backend = null;
                }

            } finally {
                // make sure if we've still got a backend object that
                // we return it to the pool
                if (backend != null) {
                    returnBackend(backend);
                }
            }

            // wait a little while before continuing
            retrySleep();

            log.info("Error storing file to mogile - attempting to reconnect and try again (attempt #" + attempt + ")");
        }

        throw new MogileException("Unable to store file on mogile after multiple attempts");
    }

    public void storeFile(final String key, final String storageClass, final File file) throws MogileException {
        try {
            FileInputStream in = new FileInputStream(file);
            storeStream(key, storageClass, in, file.length());
        } catch (IOException e) {
            log.warn("error trying to store file", e);
        }
    }

    /**
     * Read in an file and store it in the provided file. Return
     * the reference to the file, or null if we couldn't find the
     * object with the given key
     *
     * @param key
     * @param destination
     * @throws NoTrackersException
     * @throws TrackerCommunicationException
     */
    public File getFile(final String key, final File destination)
            throws NoTrackersException, TrackerCommunicationException, IOException, StorageCommunicationException {
        InputStream in = getFileStream(key);

        if (in == null) {
            return null;
        }
        try {
            OutputStream out = new FileOutputStream(destination);
            try {
                byte[] buffer = new byte[4096];
                int count = 0;
                while ((count = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, count);
                }
            } finally {
                out.close();
            }
        } finally {
            in.close();
        }

        return destination;
    }

    public byte[] getFileBytes(final String key)
            throws NoTrackersException, TrackerCommunicationException, IOException, StorageCommunicationException {
        // pull in the paths for this file
        String paths[] = getPaths(key, false);

        // does this exist?
        if (paths == null) {
            if (log.isDebugEnabled()) {
                log.debug("couldn't find paths for " + key);
            }
            return null;
        }

        // randomly pick one of the files to retrieve and if that fails, try
        // to get another one
        int startIndex = keepPathOrder ? 0 : (int) Math.floor(Math.random() * paths.length);
        int tries = paths.length;

        while (tries-- > 0) {
            String path = paths[startIndex++ % paths.length];

            try {
                URL pathURL = new URL(path);
                if (log.isDebugEnabled()) {
                    log.debug("retrieving file from " + path + " (attempt #" + (paths.length - tries) + ")");
                }

                HttpURLConnection conn = (HttpURLConnection) pathURL.openConnection();
                InputStream in = conn.getInputStream();

                byte[] bytes = new byte[conn.getContentLength()];
                int offset = 0;
                int count = 0;
                while ((offset < bytes.length) && ((count = in.read(bytes, offset, bytes.length - offset)) > 0)) {
                    // just keep reading until we've got it all
                    offset += count;
                }

                return bytes;

            } catch (IOException e) {
                log.warn("problem reading file from " + path);
            }
        }

        StringBuilder pathString = new StringBuilder();
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                pathString.append(", ");
            }
            pathString.append(paths[i]);
        }

        throw new StorageCommunicationException("unable to retrieve file from any storage node: " + pathString);
    }

    /**
     * Retrieve the data from some file. Return null if the file doesn't
     * exist, or throw an exception if we just can't get it from any
     * of the storage nodes
     *
     * @param key
     * @return
     */
    public InputStream getFileStream(final String key)
            throws NoTrackersException, TrackerCommunicationException, StorageCommunicationException {
        // pull in the paths for this file
        String paths[] = getPaths(key, false);

        // does this exist?
        if (paths == null) {
            return null;
        }

        // randomly pick one of the files to retrieve and if that fails, try
        // to get another one
        int startIndex = keepPathOrder ? 0 : (int) Math.floor(Math.random() * paths.length);
        int tries = paths.length;

        while (tries-- > 0) {
            String path = paths[startIndex++ % paths.length];

            try {
                URL pathURL = new URL(path);
                if (log.isDebugEnabled()) {
                    log.debug("retrieving file from " + path + " (attempt #" + (paths.length - tries) + ")");
                }

                return pathURL.openStream();

            } catch (IOException e) {
                log.warn("problem reading file from " + path);
            }
        }

        StringBuilder pathString = new StringBuilder();
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                pathString.append(", ");
            }
            pathString.append(paths[i]);
        }

        throw new StorageCommunicationException(
                "unable to retrieve file with key '" + key + "' from any storage node: " + pathString);
    }

    /**
     * Delete the given file. A non-existant file will not cause an error.
     *
     * @param key
     * @throws NoTrackersException
     */
    public void delete(final String key) throws NoTrackersException {
        int attempt = 1;

        Backend backend = null;

        while ((maxRetries == -1) || (attempt++ <= maxRetries)) {
            try {
                backend = borrowBackend();
                backend.doRequest("delete", new String[]{"domain", domain, "key", key});

                return;

            } catch (TrackerCommunicationException e) {
                log.warn(e.getMessage(), e);

                // don't use this any more
                if (backend != null) {
                    invalidateBackend(backend);
                    backend = null;
                }

            } finally {
                if (backend != null) {
                    returnBackend(backend);
                }
            }

            // something went wrong - so wait a little while before continuing
            retrySleep();
        }

        throw new NoTrackersException();
    }

    /**
     * Something went wrong - so wait a little while before continuing
     */
    private void retrySleep() {
        if (retrySleepTime > 0) {
            try {
                Thread.sleep(retrySleepTime);
            } catch (Exception e) {
            }
        }
    }

    /**
     * Tell the server to sleep for a few seconds.
     *
     * @throws NoTrackersException
     */
    public void sleep(final int seconds) throws NoTrackersException, TrackerCommunicationException {
        Backend backend = null;

        try {
            backend = borrowBackend();
            backend.doRequest("sleep", new String[]{"duration",
                    Integer.toString(seconds)});

        } finally {
            if (backend != null) {
                returnBackend(backend);
            }
        }

    }

    /**
     * Rename the given key. Note that you won't get an error if the key doesn't
     * exist.
     *
     * @param fromKey
     * @param toKey
     * @throws NoTrackersException
     */
    public void rename(final String fromKey, final String toKey) throws NoTrackersException {
        int attempt = 1;

        Backend backend = null;

        while ((maxRetries == -1) || (attempt++ <= maxRetries)) {
            try {
                backend = borrowBackend();

                backend.doRequest("rename", new String[]{"domain", domain,
                        "from_key", fromKey, "to_key", toKey});

                return;

            } catch (TrackerCommunicationException e) {
                log.warn(e.getMessage(), e);

                if (backend != null) {
                    invalidateBackend(backend);
                    backend = null;
                }

            } finally {
                if (backend != null) {
                    returnBackend(backend);
                }
            }

            // something went wrong - so wait a little while before continuing
            retrySleep();
        }

        throw new NoTrackersException();
    }

    /**
     * Return a list of URL's that specify where this file is stored. Return
     * null if there was an error from the server.
     *
     * @param key
     * @param noverify If true, then ask the server not to bother checking that the
     *                 paths it's going to return are valid.
     * @return array of Strings that are URLs that specify where this file is
     *         stored, or null if there was an error
     * @throws NoTrackersException
     */
    public String[] getPaths(final String key, final boolean noverify) throws NoTrackersException {
        int attempt = 1;

        Backend backend = null;

        while ((maxRetries == -1) || (attempt++ <= maxRetries)) {
            try {
                backend = borrowBackend();

                Map<String, String> response = backend.doRequest("get_paths", new String[]{"domain",
                        domain, "key", key, "noverify", (noverify ? "1" : "0")});

                if (response == null) {
                    return null;
                }

                int pathCount = Integer.parseInt(response.get("paths"));
                String[] paths = new String[pathCount];
                for (int i = 1; i <= pathCount; i++) {
                    String path = response.get("path" + i);
                    paths[i - 1] = path;
                }

                return paths;

            } catch (TrackerCommunicationException e) {
                log.warn(e.getMessage(), e);

                if (backend != null) {
                    invalidateBackend(backend);
                    backend = null;
                }

            } finally {
                if (backend != null) {
                    returnBackend(backend);
                }
            }

            // something went wrong - so wait a little while before continuing
            retrySleep();
        }

        throw new NoTrackersException();
    }

    /**
     * Return the after key and a list of keys matching your key. Return
     * null if there was an error from the server.
     *
     * @param key
     * @return array of after key and array of keys
     * @throws NoTrackersException
     */
    public Object[] listKeys(final String key, final String after, final int limit) throws NoTrackersException {
        int attempt = 1;

        Backend backend = null;

        while ((maxRetries == -1) || (attempt++ <= maxRetries)) {
            try {
                backend = borrowBackend();

                Map<String, String> response = backend.doRequest("list_keys", new String[]{"domain",
                        domain, "prefix", key, "after", after == null ? "" : after});

                if (response == null) {
                    return null;
                }

                int keyCount = Integer.parseInt(response.get("key_count"));
                String[] retKeys = new String[keyCount];
                for (int i = 1; i <= keyCount; i++) {
                    String retKey = response.get("key_" + i);
                    retKeys[i - 1] = retKey;
                }

                return new Object[]{retKeys, response.get("after_key")};

            } catch (TrackerCommunicationException e) {
                log.warn(e.getMessage(), e);

                if (backend != null) {
                    invalidateBackend(backend);
                    backend = null;
                }

            } finally {
                if (backend != null) {
                    returnBackend(backend);
                }
            }

            // something went wrong - so wait a little while before continuing
            if (retrySleepTime > 0) {
                try {
                    Thread.sleep(retrySleepTime);
                } catch (Exception e) {
                }
                ;
            }
        }

        throw new NoTrackersException();
    }

    public Object[] listKeys(final String key) throws NoTrackersException {
        return listKeys(key, null, 1000);
    }

    public Object[] listKeys(final String key, final int limit) throws NoTrackersException {
        return listKeys(key, null, limit);
    }


    public String getDomain() {
        return domain;
    }

    Backend borrowBackend() throws NoTrackersException {
        try {
            ObjectPool backendPool = getBackendPool();

            if (log.isDebugEnabled()) {
                log.debug("getting backend (active: " + backendPool.getNumActive() + ", idle: " +
                        backendPool.getNumIdle() + ")");
            }

            final Backend backend = (Backend) backendPool.borrowObject();

            if (log.isDebugEnabled()) {
                log.debug("got backend (active: " + backendPool.getNumActive() + ", idle: " + backendPool.getNumIdle() +
                        ")");
            }

            return backend;

        } catch (Exception e) {
            log.error("unable to get backend", e);
            throw new NoTrackersException();
        }
    }

    void returnBackend(final Backend backend) {
        try {
            ObjectPool backendPool = getBackendPool();

            if (log.isDebugEnabled()) {
                log.debug("returning backend (active: " + backendPool.getNumActive() + ", idle: " +
                        backendPool.getNumIdle() + ")");
            }

            backendPool.returnObject(backend);

            if (log.isDebugEnabled()) {
                log.debug("returned backend (active: " + backendPool.getNumActive() + ", idle: " +
                        backendPool.getNumIdle() + ")");
            }

        } catch (Exception e) {
            // I think we can ignore this.
            log.warn("unable to return backend", e);
        }
    }

    void invalidateBackend(final Backend backend) {
        try {
            ObjectPool backendPool = getBackendPool();

            if (log.isDebugEnabled()) {
                log.debug("invalidating backend (active: " + backendPool.getNumActive() + ", idle: " +
                        backendPool.getNumIdle() + ")");
            }

            backendPool.invalidateObject(backend);

            if (log.isDebugEnabled()) {
                log.debug("invalidated backend (active: " + backendPool.getNumActive() + ", idle: " +
                        backendPool.getNumIdle() + ")");
            }

        } catch (Exception e) {
            // I think we can ignore this
            log.warn("unable to invalidate backend", e);
        }
    }


}

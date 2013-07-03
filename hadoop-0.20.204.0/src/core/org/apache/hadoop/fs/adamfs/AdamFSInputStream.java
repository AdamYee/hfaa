package org.apache.hadoop.fs.adamfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

class AdamFSInputStream extends FSInputStream {
	public static final Log LOG = LogFactory.getLog(AdamFSInputStream.class);
	private long position = 0;
	// private int buffersize;
	private long fileLength;
	private String defaultname;

	private Socket apiSocket;
	private PrintWriter out = null;
	private InputStream in = null;

	public AdamFSInputStream(String fsDefaultName, Socket s, long flen)
			throws IOException {
		defaultname = fsDefaultName;
		apiSocket = s;
		fileLength = flen;
		in = apiSocket.getInputStream();
		out = new PrintWriter(apiSocket.getOutputStream(), true);
//		LOG.info("<AdamFS:INPUTSTREAM> Constructed!");
//		LOG.info("<AdamFS:INPUTSTREAM> file size: "+fileLength);
	}

	@Override
	public int read() throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:read>");
		byte[] buf = new byte[1];
		int ret;
		if (getPos() >= fileLength)
			return -1;
		ret = read(buf, 0, 1); // will call our own read below
		if (ret == -1)
			return ret;
		return ((int) buf[0]) & 0xFF;
	}

	@Override
	public int read(byte[] b) throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:read-b>");
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte b[], int off, int length) throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:read-b-off-length>");
		if (b == null) {
			throw new NullPointerException();
		} else if (off < 0 || length < 0 || length > b.length - off) {
			throw new IndexOutOfBoundsException();
		} else if (length == 0) {
			return 0;
		}
		
		// send request command to fsapi-server
		// prompt read bytes
		out.println(position + ":" + length); // *** RP6 ***
		// immediately receive data, *** RP8 ***
		int c = in.read(); // read first byte
		if (c == -1) {
			return -1;
		}
		b[off] = (byte) c;
		int i = 1;
		if (length > 1) {
			off += i;
			length -= off;
			i += in.read(b, off, length);
		}
		position += i; // update position in file
//		LOG.info("<AdamFS:INPUTSTREAM:read-b-off-length> read "+i+"bytes");
		return i;
	}

	@Override
	public int read(long position, byte[] buffer, int offset, int length)
			throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:read-pos-buffer-offset-length>");
		this.position = position;
		return read(buffer, offset, length);
	}

	@Override
	public void readFully(long position, byte[] buffer, int offset, int length)
			throws IOException {
	}

	@Override
	public void readFully(long position, byte[] buffer) throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:readFully-pos-buffer>");
	}

	@Override
	public void seek(long pos) throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:seek> to position: " + pos);
		position = pos;
	}

	@Override
	public long getPos() throws IOException {
//		LOG.info("<AdamFS:INPUTSTREAM:getPos> returned: " + position);
		return position;
	}

	/**
	 * Most likely will not need to use this function
	 */
	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}

	private Path makeAbsolute(Path f) {
		if (f == null)
			return new Path("/");
		if (f.toString().startsWith(defaultname)) {
			Path stripped_path = new Path(f.toString()
					.substring(defaultname.length()));
			return stripped_path;
		}
		return f;
	}
}
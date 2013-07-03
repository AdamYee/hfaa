package org.apache.hadoop.fs.adamfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class AdamFSOutputStream extends DataOutputStream {
	// This class is used to wrap a SocketOutputStream in case
	// there is extra functionality needed
	private String path;

	protected AdamFSOutputStream(OutputStream out, String f) throws IOException, SecurityException {
		super(out);
		setPath(f);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}
	
}

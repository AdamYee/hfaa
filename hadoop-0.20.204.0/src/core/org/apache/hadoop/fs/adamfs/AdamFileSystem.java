package org.apache.hadoop.fs.adamfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class AdamFileSystem extends FileSystem {
	// ---------------------------------------------------------------
	// File system interface for Hadoop which connects to
	// a user implemented network API that follows the adamfs protocol.
	// Handles requests and responses between Hadoop and
	// the other file system. Metadata is encapsulated in TCP packets
	// specified by the adamfs protocol while read/write streams are 
	// handled through socket streams.
	// ---------------------------------------------------------------

	private URI uri;
	private Path workingDir;
	private static String fs_default_name;

	private Socket apiSocket = null;
	private PrintWriter out = null;
	private BufferedReader in = null;

	AdamFileSystem() throws UnknownHostException, IOException {
		//LOG.info("<AdamFS> AdamFileSystem constructor called");
	}

	@Override
	public URI getUri() {
		return this.uri;
	}

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		setConf(conf);
		fs_default_name = conf.get("fs.default.name", "file:///");
		//LOG.info("<AdamFS> fs.default.name: " + fs_default_name);
		this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
		//LOG.info("<AdamFS> uri at initialize: " + uri);
		// conf.set("mapred.system.dir", "/home/adam/hadoop/AdamFS/temp");
		this.workingDir = new Path("/user", System.getProperty("user.name"))
				.makeQualified(this);
		//LOG.info("<AdamFS> workingDir: " + this.workingDir);
		statistics = getStatistics(uri.getScheme(), getClass());
		initiate_connection();
		out.println("AdamFS SAYS HI, URI: "+this.uri);
		close_connection();
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		//LOG.info("<AdamFS:open>");
		//LOG.info("<AdamFS:open> opening stream for file: " + f);
		Path abs_path = makeAbsolute(f);
		// Tell file system api-server to stream data this way
		initiate_connection();
		send_command("07", abs_path.toString()); // RP1
		String response = get_response(); // RP4
		// Don't close the socket connect, Hadoop will close it after inputstream ends
		// Parse the response
		String resp_id = response.substring(0, 2);
		String success_code = response.substring(2, 3); // did we open the file?
		String data = response.substring(3, response.length()); // total file length
		//log_response(resp_id, success_code, data, "open");
		if (success_code.compareTo("1") == 0) {
			//LOG.info("<AdamFS:open> "+f.toString()+" opened");
			//LOG.info("<AdamFS:open> reading data in from AdamFSInputStream");
			// Return the SocketInputStream
			return new FSDataInputStream(
					new AdamFSInputStream(this.fs_default_name.toString(),
							this.apiSocket, Long.parseLong(data))); // *** RP5 ***
		} else {
			// if the file system failed to open the file
			close_connection();
			throw new IOException("<AdamFS:open> file system failed to open file");
		}
	}

	@Override
	// Create a new file and open an FSDataOutputStream that's connected to it.
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication, long blockSize,
			Progressable progress) throws IOException {
		//LOG.info("<AdamFS:create>");
		//LOG.info("<AdamFS:create> file: "+f);
		// What does a Progressable do?
		
		Path abs_path = makeAbsolute(f);
		String response = null;
		String resp_id = null;
		String success_code = null;
		
		// Step 1: existence test
		boolean exists = exists(f);
		if (exists) {
			FileStatus fstat = getFileStatus(f);
			if (fstat.isDir()) {
				throw new IOException("<AdamFS:create> cannot overwrite directory: " + f.toString());
			}
			else if (!overwrite) {
				throw new IOException("<AdamFS:create> cannot overwrite file : " + f.toString() + 
						"without overwrite flag set to true");
			}
		}
		
		// Step 2: create any nonexistent directories in the path
		if (!exists) {
			// get the parent path
			Path parent = abs_path.getParent();
			if (parent != null) {
				if (!this.mkdirs(new Path("adamfs", "localhost:9999", parent.toString()), permission)) {
					throw new IOException("<AdamFS:create> error creating directory: " + f.toString());
				}
			}
		}
		
		// Step 3: open the file for writing
		// create file to write to if it doesn't exist already
		if (!exists) {
			response = init_send_get_close("05", abs_path.toString()); // CREATE FILE
			// Parse the response
			resp_id = response.substring(0, 2);
			success_code = response.substring(2, 3);
			//log_response(resp_id, success_code, "", "create");
		} else {
			// file exists and is enabled to overwrite
			success_code = "1"; // go ahead and begin streaming
		}
		// If the file was created, write to it
		if (success_code.compareTo("1") == 0) { // success_code 1
			//LOG.info("<Adamfs:create> successfully created file: "+abs_path.toString()+" for OutputStream");
			// Step 4: create and return the stream
			// Send a command to api-server indicating Hadoop is going to
			// stream bytes. Design api-server to receive command,
			// then process streamed bytes in a loop until outstream closes. 
			initiate_connection();
			send_command("06", abs_path.toString()); // SETUP WRITE
			response = get_response();
			// Don't close socket connection, it will get closed by the OutputStream
			// Parse the response
			resp_id = response.substring(0, 2);
			success_code = response.substring(2, 3);
			//log_response(resp_id, success_code, "", "create");
			if (success_code.compareTo("1") == 0) {
				//LOG.info("<AdamFS:create> writing data out SocketOutputStream to file system");
				// Return the SocketOutputStream, Hadoop will close it when done
				return new FSDataOutputStream(new AdamFSOutputStream(
						apiSocket.getOutputStream(), abs_path.toString()), statistics);
			} else {
				close_connection(); // Failed to open the file
				throw new IOException("<AdamFS:create> failed to establish connection for socket stream");
			}
		} else { // success_code 0
			// Other file system failed to create the file
			throw new IOException("<AdamFS:create> failed to create file: " + abs_path.toString() +
														" for OutputStream");
		}
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
			throws IOException {
		FileStatus fstat = getFileStatus(f);
		return this.create(f, fstat.getPermission(), false, bufferSize,
				fstat.getReplication(), fstat.getBlockSize(), progress);
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
//		LOG.info("<AdamFS:RENAME> !!!");
		String src_abs = makeAbsolute(src).toString();
		String dst_abs = makeAbsolute(dst).toString();
//		LOG.info("<AdamFS:RENAME> src: "+src_abs+", dst: "+dst_abs);
		String response = init_send_get_close("09", src_abs+":"+dst_abs);
		String resp_id = response.substring(0, 2);
		String success_code = response.substring(2, 3);
		//log_response(resp_id, success_code, "", "rename");
		boolean result = false;
		if (success_code.compareTo("1") == 0) { // success_code 1
			result = true;
		} else {
			if (getFileStatus(dst).isDir()) { // move srcdir into dstdir
				Path new_dst = new Path(dst, src.getName());
				result = rename(src, new_dst);
//				LOG.info("<AdamFS:RENAME> moving srcdir result: "+result);
			}
		}
		return result;
	}

	@Override
	@Deprecated
	public boolean delete(Path f) throws IOException {
		return delete(f, true);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		/* If Path f is a single file, delete
		 * Else if isDir, recursively delete children before deleting directory f
		 * */
		//LOG.info("<AdamFS:delete>");
		String path = makeAbsolute(f).toString();
		String response = null;
		String resp_id = null;
		String success_code = null;
		String data = null;
		
		try {
			FileStatus fstat = this.getFileStatus(f);
			if (!fstat.isDir()) {
				// Delete a single file
				//LOG.info("<AdamFS:delete> deleting single file: "+path);
				response = init_send_get_close("03", path); // DELETE
				// Parse the response
				resp_id = response.substring(0, 2);
				success_code = response.substring(2, 3);
				//log_response(resp_id, success_code, "", "delete");
				if (success_code.compareTo("1") == 0) { // success_code 1
					return true; // FILE DELETED
				} else { // success_code 0
					// Other file system failed to delete
					return false;
				}
				
			} else {
				// Recursively delete directory contents
				//LOG.info("<AdamFS:delete> recursively deleting directory contents in: "+path);

				response = init_send_get_close("02", path);
				// Parse the response
				resp_id = response.substring(0, 2);
				success_code = response.substring(2, 3);
				data = response.substring(3, response.length());
				//log_response(resp_id, success_code, data, "delete");
				if (success_code.compareTo("1") == 0) { // list-files succeeded
					if (data.compareTo("") == 0) {
						// there are no files to delete in this directory,
						// so delete the directory itself
						//LOG.info("<AdamFS:delete> finally deleting directory: "+path);
						response = init_send_get_close("03", path); // DELETE
						// Parse the response
						resp_id = response.substring(0, 2);
						success_code = response.substring(2, 3);
						//log_response(resp_id, success_code, "", "delete");
						if (success_code.compareTo("0") == 0) {
							// failed to delete
							return false;
						}
						return true; // DIRECTORY DELETED
					} else {
						// delete the array of containing files/directories
						String[] files = data.split("[ ]"); // array of filenames and/or directories
						for (int i=0; i<files.length; i++) {
							// don't forget to prepend the original path so other file system
							// knows what to delete
							this.delete(new Path("adamfs", "localhost:9999", path+"/"+files[i]), true);
						}
						// finally delete the directory that did have files
						return this.delete(new Path("adamfs", "localhost:9999", path), true);
					}
				} else { // success code 0 
					// failed to list files...bad if we get here
					return false;
				}		
			} // end if isDir()
		} catch (Exception e) {
			//LOG.info("<AdamFS:delete> failed to delete file because file did not exist.");
			return false;
		}
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		//LOG.info("<AdamFS:listStatus>");
		try {
			FileStatus fstat = this.getFileStatus(f);
			if (!fstat.isDir()) {
				//LOG.info("<AdamFS:listStatus> file IS NOT a directory");
				return new FileStatus[]{fstat};
			} else {
				//LOG.info("<AdamFS:listStatus> file IS a directory");
				String path = makeAbsolute(f).toString();
				FileStatus[] ret = null;
				
				String response = init_send_get_close("02", path);
				
				// Parse the response
				String resp_id = response.substring(0, 2);
				String success_code = response.substring(2, 3);
				String data = response.substring(3, response.length());
				//log_response(resp_id, success_code, data, "listStatus");
				if (success_code.compareTo("1") == 0) {
					String[] files = data.split("[ ]"); // array of files
					ret = new FileStatus[files.length]; // populate array with empty FileStatus objects
					for (int i=0; i<files.length; i++) {
						// create a new FileStatus object for each file
						ret[i] = this.getFileStatus(new Path("adamfs", "localhost:9999", path+"/"+files[i])); 
					}
					return ret;
				} else { // 0
					// response success code = failure
					return new FileStatus[0];
				}
			} // end if isDir
		} catch (Exception e) {
			//LOG.info("<AdamFS:listStatus> failed to list file status because file did not exist.");
			return new FileStatus[0];
		}
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		this.workingDir = new_dir;
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		//LOG.info("<AdamFS:mkdirs>");
		//LOG.info("<AdamFS:mkdirs> creating directory Path f: " + f);
		if (exists(f)) {
			return true;
		}
		/*
		 * Interesting note from HDFS: all the users of mkdirs() are used to expect
		 * 'true' even if a new directory is not created.
		 */
//		URI uri = f.toUri();
		String path = makeAbsolute(f).toString();
		
		String response = init_send_get_close("04", path); // MKDIR
		
		// Parse the response
		String resp_id = response.substring(0, 2);
		String success_code = response.substring(2, 3);
		//log_response(resp_id, success_code, "", "mkdirs");
		if (success_code.compareTo("1") == 0) {
			// return true after the file has been created
			return exists(f);
		} else {
			// However, really return false if the directory was not creating successfully
			return false;
		}
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		//LOG.info("<AdamFS:getFileStatus>");
		//LOG.info("<AdamFS:getFileStatus> Path f: " + f);
		String path = makeAbsolute(f).toString();
		
		String response = init_send_get_close("01", path); // LOOKUP

		// Parse the response
		String resp_id = response.substring(0, 2);
		String success_code = response.substring(2, 3);
		String data = response.substring(3, response.length());
		//log_response(resp_id, success_code, data, "getFileStatus");
		
		if (success_code.compareTo("1") == 0) {
			String[] params = data.split("[ ]");
			Boolean bool = false;
			if (params[1].compareTo("1") == 0) {
				bool = true;
			}
			return new FileStatus(Long.parseLong(params[0]), // long length
					bool, // boolean isdir
					1, // int block_replication always 1
					Long.parseLong(params[2]), // long blocksize
					Long.parseLong(params[3]), // long mod_time
					Long.parseLong(params[4]), // long access_time
					new FsPermission(params[5]), // FsPermission permission
					params[6], // String owner
					params[7], // String group
					f); // Path path
		} else {
			throw new FileNotFoundException("File does not exist: " + f);
		}
	}
	
//	@Override
//	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
//			long len) throws IOException {
//		String path = makeAbsolute(file.getPath()).toString();
//		String response = init_send_get_close("08", path); // GET FILE LOCATIONS
//		return null;
//	}
	
	//Utility functions
	
	private void log_response(String resp_id, String success_code, String data,
			String method) {
		LOG.info("<AdamFS:" + method + "> resp_id: " + resp_id + ", success_code: "
				+ success_code + ", data: " + data);
	}
	
	public Path makeAbsolute(Path f) {
		if (f == null)
			return new Path("/");
		if (f.toString().startsWith(this.fs_default_name)) {
			Path stripped_path = new Path(f.toString().substring(
					fs_default_name.length()));
			return stripped_path;
		}
		return f;
	}


	// Network API functions

	private void initiate_connection() throws UnknownHostException, IOException {
		this.apiSocket = new Socket("localhost", 9999);
		this.in = new BufferedReader(new InputStreamReader(apiSocket.getInputStream()));
		this.out = new PrintWriter(apiSocket.getOutputStream(), true);
	}
	
	private void send_command(String req_id, String data) {
		this.out.println(req_id + data);
	}
	
	private String get_response() throws IOException {
		// Note: this must receive a newline or carriage return
		return in.readLine();
	}
	
	private void close_connection() throws IOException {
		in.close();
		out.close();
		apiSocket.close();
	}
	
	private String init_send_get_close(String req_id, String data) throws UnknownHostException, IOException {
		initiate_connection();
		send_command(req_id, data);
		String response = get_response();
		close_connection();
		return response;
	}
} // end class

package brugidou.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class ConfigServer {

	private class ServeHdfsConfigHandler extends AbstractHandler {
		public void handle(String target, Request baseRequest, HttpServletRequest request,HttpServletResponse response) throws IOException {

			String plainFilename = "";
			try {
				plainFilename = new File(new URI(target).getPath()).getName();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			File confFile = new File(confDir + "/" + plainFilename);

			if (!confFile.exists()) {
				throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath() + ". Please make sure it exists.");
			}

			String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())));
			content = content.replace("${namenodeAddress}", namenodeAddress);

			response.setContentType("application/octet-stream;charset=utf-8");
			response.setHeader("Content-Disposition", "attachment; filename=\"" + plainFilename + "\" ");
			response.setHeader("Content-Transfer-Encoding", "binary");
			response.setHeader("Content-Length", Integer.toString(content.length()));

			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			response.getWriter().println(content);
		}

	}


	private Server server;
	private String confDir;
	private int port;

	private String namenodeAddress;

	public ConfigServer(Configuration conf) throws Exception {
		confDir = "conf";
		port = conf.getInt("mesos.hdfs.conf.port", 8283);

		namenodeAddress = conf.get("mesos.hdfs.namenode.hostname", InetAddress.getLocalHost().getHostName())
				 + ":"
				 + conf.getInt("mesos.hdfs.namenode.port", 8020);


		server = new Server(port);
		server.setHandler(new ServeHdfsConfigHandler());
		server.start();
	}

	public void stop() throws Exception {
	    server.stop();
	}

}

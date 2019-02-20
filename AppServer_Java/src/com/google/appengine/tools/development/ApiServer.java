package com.google.appengine.tools.development;

import com.google.appengine.repackaged.org.apache.commons.httpclient.HttpClient;
import com.google.appengine.repackaged.org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import com.google.appengine.repackaged.org.apache.commons.httpclient.methods.GetMethod;
import com.google.appengine.repackaged.org.apache.commons.httpclient.methods.PostMethod;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Request;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Response;
import com.google.apphosting.utils.runtime.ApiProxyUtils;
import com.google.net.util.proto2api.Status.StatusProto;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.ws.http.HTTPException;

public class ApiServer {
    private static final Logger logger = Logger.getLogger(ApiServer.class.getName());
    private final Process process;
    private final int port;

    ApiServer(String pathToApiServer) {
        try {
            ServerSocket socket = new ServerSocket(0);
            Throwable error = null;

            try {
                this.port = socket.getLocalPort();
            } catch (Throwable e) {
                error = e;
                throw e;
            } finally {
                if (error != null) {
                    try {
                        socket.close();
                    } catch (Throwable e) {
                        error.addSuppressed(e);
                    }
                } else {
                    socket.close();
                }

            }

            String[] cmd = new String[]{
                    pathToApiServer,
                    "--api_port", String.valueOf(this.port),
                    "--clear_datastore",
                    "--datastore_consistency_policy", "consistent",
                    "--application", "test",
                    "--application_prefix", "",
                    "--datastore_path", (new StringBuilder(16)).append("/tmp/").append(this.port).toString()};

            this.process = new ProcessBuilder().command(cmd).redirectErrorStream(true).start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8));

            String stdInputLine;
            while((stdInputLine = stdInput.readLine()) != null && !stdInputLine.contains("Starting API server at:")) {
                ;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // AppScale: Use an existing server without starting it.
    ApiServer(int externalServerPort) {
        this.port = externalServerPort;
        this.process = null;
    }

    public Integer getPort() {
        return this.port;
    }

    public void clear() throws IOException {
        HttpClient httpClient = new HttpClient();
        int returnCode = this.port;
        GetMethod request = new GetMethod((new StringBuilder(34)).append("http://localhost:").append(returnCode).append("/clear").toString());
        returnCode = httpClient.executeMethod(request);
        if (returnCode != 200) {
            throw new IOException((new StringBuilder(78)).append("Sending HTTP request to clear the API server failed with response: ").append(returnCode).toString());
        }
    }

    public void close() {
        // AppScale: If a server was not started, there is nothing to stop.
        if (this.process == null) {
            return;
        }

        try {
            int exitValue = this.process.exitValue();
            if (exitValue != 0) {
                logger.logp(Level.WARNING, "com.google.appengine.tools.development.ApiServer", "close", (new StringBuilder(67)).append("The API server process exited with a non-zero value of: ").append(exitValue).toString());
            }
        } catch (IllegalThreadStateException var2) {
            this.process.destroy();
        }

    }

    byte[] makeSyncCall(String packageName, String methodName, byte[] requestBytes) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        Request remoteApiRequest = new Request();
        remoteApiRequest.setServiceName(packageName);
        remoteApiRequest.setMethod(methodName);
        remoteApiRequest.setRequestAsBytes(requestBytes);
        remoteApiRequest.setRequestId(UUID.randomUUID().toString().substring(0, 10));
        byte[] remoteApiRequestBytes = ApiUtils.convertPbToBytes(remoteApiRequest);
        PostMethod post = new PostMethod((new StringBuilder(28)).append("http://localhost:").append(this.port).toString());
        post.setFollowRedirects(false);
        post.addRequestHeader("Host", "localhost");
        post.addRequestHeader("Content-Type", "application/octet-stream");
        post.setRequestEntity(new ByteArrayRequestEntity(remoteApiRequestBytes));

        try {
            HttpClient httpClient = new HttpClient();
            httpClient.executeMethod(post);
            if (post.getStatusCode() != 200) {
                throw new HTTPException(post.getStatusCode());
            }
        } catch (IOException e) {
            throw new IOException("Error executing POST to HTTP API server.");
        }

        Response response = new Response();
        boolean parsed = response.mergeFrom(post.getResponseBodyAsStream());
        if (!parsed) {
            throw new IOException("Error parsing the response from the HTTP API server.");
        } else if (response.hasApplicationError()) {
            throw ApiProxyUtils.getRpcError(packageName, methodName, StatusProto.getDefaultInstance(), response.getApplicationError().getCode(), response.getApplicationError().getDetail());
        } else {
            return response.getResponseAsBytes();
        }
    }
}

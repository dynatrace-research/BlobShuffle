package com.dynatrace.research.s3shuffler;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import io.javalin.Javalin;
import io.javalin.http.Header;
import io.javalin.http.HttpStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class ProxyingS3BatchFacade implements S3BatchFacade, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyingS3BatchFacade.class);

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

    private final Javalin webserver;

    private final MemberDiscoverer memberDiscoverer;

    private final HttpClient httpClient = HttpClient.newBuilder().build();

    private final S3BatchFacade wrapped;

    private final MeterRegistry metricRegistry;

    //private final Counter cacheHitsCounter;
    //private final Counter cacheMissesCounter;
    //private final Timer cacheReadTimer;

    public ProxyingS3BatchFacade(S3BatchFacade wrapped, String zone, String advertisedHost, int port, MeterRegistry meterRegistry) {
        this(wrapped, port, new JGroupsMemberDiscoverer(zone, advertisedHost + ':' + port), meterRegistry);
    }

    public ProxyingS3BatchFacade(S3BatchFacade wrapped, int port, MemberDiscoverer memberDiscoverer, MeterRegistry meterRegistry) {
        this.wrapped = wrapped;

        this.webserver = Javalin.create(config -> {
                    config.useVirtualThreads = true;
                    config.showJavalinBanner = false;
                })
                .get("/batches/<objectPath>", ctx -> {
                    String objectPath = ctx.pathParam("objectPath");
                    //ctx.queryParam("offsetStart");
                    //Integer.valueOf(ctx.queryParam("offsetEnd"));
                    try (SizeAwareInputStream readBatchStream = this.readBatch(objectPath)) {
                        ctx.header(Header.CONTENT_LENGTH, String.valueOf(readBatchStream.size()));
                        ctx.result(readBatchStream);
                    }
                })
                .put("/batches/<objectPath>", ctx -> {
                    String objectPath = ctx.pathParam("objectPath");
                    int objectSize = ctx.contentLength();
                    try (InputStream inputStream = ctx.bodyInputStream()) {
                        this.writeBatch(objectPath, inputStream, objectSize);
                    }
                    ctx.status(HttpStatus.CREATED);
                })
                .start(port);
        this.memberDiscoverer = memberDiscoverer;

        this.metricRegistry = meterRegistry;

    }

    private String getOwnerForObjectPath(String objectPath) {
        int objectPathHash = hasher.hashStream().putString(objectPath).getAsInt();
        List<String> members = memberDiscoverer.getMembers().stream().sorted().toList();
        //Collections.sort(members);
        return members.get(Utils.toPositive(objectPathHash) % members.size());
    }

    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        String objectOwner = this.getOwnerForObjectPath(objectPath);
        if (objectOwner.equals(this.memberDiscoverer.getThisMember())) {
            this.wrapped.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
        } else {
            LOGGER.info("Forward write request for object '{}' to '{}'", objectPath, objectOwner);
            this.forwardWriteBatch(objectPath, batchInputStream, batchSizeInBytes, objectOwner);
        }
    }

    private void forwardWriteBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes, String objectOwner) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(("http://" + objectOwner + "/batches/" + objectPath)))
                //.header("Content-Length", String.valueOf(batchSizeInBytes))
                .PUT(new SizeAwareBodyPublisher(HttpRequest.BodyPublishers.ofInputStream(() -> batchInputStream), batchSizeInBytes)) // TODO retry will not work
                .build();
        try {
            HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HttpStatus.CREATED.getCode()) {
                throw new RuntimeException("Request to member " + objectOwner + " resulted in unexpected status code " + response.statusCode());
            }
        } catch (ConnectException e) {
            throw new RuntimeException("Failed to connect to member "+ objectOwner, e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public SizeAwareInputStream readBatch(String objectPath) {
        String objectOwner = this.getOwnerForObjectPath(objectPath);
        if (objectOwner.equals(this.memberDiscoverer.getThisMember())) {
            return this.wrapped.readBatch(objectPath);
        } else {
            LOGGER.info("Forward read request for object '{}' to '{}'", objectPath, objectOwner);
            return forwardReadBatch(objectPath, objectOwner);
        }
    }

    private SizeAwareInputStream forwardReadBatch(String objectPath, String objectOwner) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(("http://" + objectOwner + "/batches/" + objectPath)))
                //.uri(URI.create(("http://" + objectOwner + "/batches/" + objectPath + "?offsetStart=" + offsetStart + "&offsetEnd=" + offsetEnd)))
                .GET()
                .build();
        try {
            HttpResponse<InputStream> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != HttpStatus.OK.getCode()) {
                throw new RuntimeException("Request to member " + objectOwner + " resulted in unexpected status code " + response.statusCode());
            }
            int contentLength = Integer.parseInt(response.headers().firstValue("Content-Length").orElseThrow());
            return new SizeAwareInputStream(response.body(), contentLength);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.webserver.stop();
    }

    private static class JGroupsMemberDiscoverer implements MemberDiscoverer {

        private final JChannel channel;

        public JGroupsMemberDiscoverer(String zone, String advertisedHostPort) {
            try {
                this.channel = new JChannel("jgroups-kubernetes.xml");
                //this.channel = new JChannel("jgroups-udp.xml");
                this.channel.setName(advertisedHostPort);
                this.channel.connect("batches-cache-" + zone);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getMembers() {
            return this.channel
                    .getView()
                    .getMembers()
                    .stream()
                    .map(Object::toString)
                    .toList();
        }

        @Override
        public String getThisMember() {
            return this.channel.address().toString();
        }

    }

}

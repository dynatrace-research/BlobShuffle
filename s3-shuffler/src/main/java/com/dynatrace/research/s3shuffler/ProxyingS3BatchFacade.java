/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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

public class ProxyingS3BatchFacade implements S3BatchFacade {

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
                    String offsetStart = ctx.queryParam("offsetStart"); // Can be null
                    String offsetEnd = ctx.queryParam("offsetEnd");  // Can be null
                    if (offsetStart != null && offsetEnd != null) {
                        int offsetStartInt = Integer.parseInt(offsetStart);
                        int offsetEndInt = Integer.parseInt(offsetEnd);
                        try (SizeAwareInputStream readPartialBatchStream = this.readPartialBatch(objectPath, offsetStartInt, offsetEndInt)) {
                            ctx.header(Header.CONTENT_LENGTH, String.valueOf(readPartialBatchStream.size()));
                            ctx.result(readPartialBatchStream);
                        }
                    } else {
                        try (SizeAwareInputStream readBatchStream = this.readBatch(objectPath)) {
                            ctx.header(Header.CONTENT_LENGTH, String.valueOf(readBatchStream.size()));
                            ctx.result(readBatchStream);
                        }
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
                throw new RuntimeException("Write request to member " + objectOwner + " resulted in unexpected status code " + response.statusCode());
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

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        String objectOwner = this.getOwnerForObjectPath(objectPath);
        if (objectOwner.equals(this.memberDiscoverer.getThisMember())) {
            return this.wrapped.readPartialBatch(objectPath, offsetStart, offsetEnd);
        } else {
            LOGGER.info("Forward partial read request for object '{}' to '{}'", objectPath, objectOwner);
            return forwardReadBatch(objectPath, objectOwner, offsetStart, offsetEnd);
        }
    }

    private SizeAwareInputStream forwardReadBatch(String objectPath, String objectOwner) {
        return forwardReadBatch(
                URI.create("http://" + objectOwner + "/batches/" + objectPath));
    }

    private SizeAwareInputStream forwardReadBatch(String objectPath, String objectOwner,  int offsetStart, int offsetEnd) {
        return forwardReadBatch(
                URI.create("http://" + objectOwner + "/batches/" + objectPath + "?offsetStart=" + offsetStart + "&offsetEnd=" + offsetEnd));
    }

    private SizeAwareInputStream forwardReadBatch(URI uri) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .build();
        try {
            HttpResponse<InputStream> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != HttpStatus.OK.getCode()) {
                throw new RuntimeException("Read request to member " + uri.getHost() + ":" + uri.getPort() + " resulted in unexpected status code " + response.statusCode());
            }
            int contentLength = Integer.parseInt(response.headers().firstValue("Content-Length").orElseThrow());
            return new SizeAwareInputStream(response.body(), contentLength);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        this.webserver.stop();
        this.memberDiscoverer.close();
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

        @Override
        public void close() {
            this.channel.close();
        }
    }

}

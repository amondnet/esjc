package com.github.msemys.esjc.node.cluster;

import com.github.msemys.esjc.node.EndpointDiscoverer;
import com.github.msemys.esjc.node.NodeEndpoints;
import com.github.msemys.esjc.util.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.stream.JsonReader;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.github.msemys.esjc.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class ClusterEndpointDiscoverer implements EndpointDiscoverer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterEndpointDiscoverer.class);

    private final ScheduledExecutorService scheduler;
    private final AtomicReference<List<MemberInfoDto>> oldGossip = new AtomicReference<>();
    private final ClusterNodeSettings settings;
    private final Gson gson;
    private final OkHttpClient client = getUnsafeOkHttpClient();



    public ClusterEndpointDiscoverer(ClusterNodeSettings settings, ScheduledExecutorService scheduler) {
        checkNotNull(settings, "settings is null");
        checkNotNull(scheduler, "scheduler is null");

        this.settings = settings;
        this.scheduler = scheduler;

        gson = new GsonBuilder()
            .registerTypeAdapter(Instant.class,
                (JsonDeserializer<Instant>) (json, type, ctx) -> Instant.parse(json.getAsJsonPrimitive().getAsString()))
            .create();
    }

    @Override
    public CompletableFuture<NodeEndpoints> discover(InetSocketAddress failedTcpEndpoint) {
        CompletableFuture<NodeEndpoints> result = new CompletableFuture<>();

        if (settings.maxDiscoverAttempts != 0) {
            scheduler.execute(() -> discover(result, failedTcpEndpoint, 1));
        } else {
            result.completeExceptionally(new ClusterException("Cluster endpoint discover is not enabled."));
        }

        return result;
    }

    private void discover(CompletableFuture<NodeEndpoints> result, InetSocketAddress failedEndpoint, int attempt) {
        final String attemptInfo = (settings.maxDiscoverAttempts != -1) ?
            String.format("%d/%d", attempt, settings.maxDiscoverAttempts) : String.valueOf(attempt);

        try {
            Optional<NodeEndpoints> nodeEndpoints = tryDiscover(failedEndpoint);

            if (nodeEndpoints.isPresent()) {
                logger.info("Discovering attempt {} successful: best candidate is {}.", attemptInfo, nodeEndpoints.get());
                result.complete(nodeEndpoints.get());
            } else {
                logger.info("Discovering attempt {} failed: no candidate found.", attemptInfo);
            }
        } catch (Exception e) {
            logger.info("Discovering attempt {} failed.", attemptInfo, e);
        }

        if (!result.isDone() && (attempt < settings.maxDiscoverAttempts || settings.maxDiscoverAttempts == -1)) {
            scheduler.schedule(() -> discover(result, failedEndpoint, attempt + 1), settings.discoverAttemptInterval.toMillis(), MILLISECONDS);
        } else {
            result.completeExceptionally(new ClusterException(String.format("Failed to discover candidate in %d attempts.", attempt)));
        }
    }

    private Optional<NodeEndpoints> tryDiscover(InetSocketAddress failedEndpoint) {
      List<MemberInfoDto> oldGossipCopy = oldGossip.getAndSet(null);

        List<GossipSeed> gossipCandidates = (oldGossipCopy != null) ?
            getGossipCandidatesFromOldGossip(oldGossipCopy, failedEndpoint) : getGossipCandidatesFromDns();

        for (GossipSeed gossipCandidate : gossipCandidates) {
            Optional<ClusterInfoDto> gossip = tryGetGossipFrom(gossipCandidate)
                .filter(c -> c.members != null && !c.members.isEmpty());

            if (gossip.isPresent()) {
                Optional<NodeEndpoints> bestNode = tryDetermineBestNode(gossip.get().members);

                if (bestNode.isPresent()) {
                    oldGossip.set(gossip.get().members);
                    return bestNode;
                }
            }
        }

        return Optional.empty();
    }

    private List<GossipSeed> getGossipCandidatesFromDns() {
        List<GossipSeed> endpoints;

        if (!settings.gossipSeeds.isEmpty()) {
            endpoints = new ArrayList<>(settings.gossipSeeds);
        } else {
            endpoints = resolveDns().stream()
                .map(address -> new GossipSeed(new InetSocketAddress(address, settings.externalGossipPort)))
                .collect(Collectors.toList());
        }

        if (endpoints.size() > 1) {
            Collections.shuffle(endpoints);
        }

        return endpoints;
    }

    private List<InetAddress> resolveDns() {
        try {
            InetAddress[] addresses = InetAddress.getAllByName(settings.dns);

            if (addresses == null || addresses.length == 0) {
                throw new ClusterException(String.format("DNS entry '%s' resolved into empty list.", settings.dns));
            } else {
                return asList(addresses);
            }
        } catch (Exception e) {
            throw new ClusterException(String.format("Error while resolving DNS entry '%s'.", settings.dns), e);
        }
    }

    private List<GossipSeed> getGossipCandidatesFromOldGossip(List<MemberInfoDto> oldGossip, InetSocketAddress failedTcpEndpoint) {
        List<MemberInfoDto> gossipCandidates = (failedTcpEndpoint == null) ? oldGossip : oldGossip.stream()
            .filter(m -> {
                try {
                    return !(m.externalTcpPort == failedTcpEndpoint.getPort() && InetAddress.getByName(m.externalTcpIp).equals(failedTcpEndpoint.getAddress()));
                } catch (UnknownHostException e) {
                    throw Throwables.propagate(e);
                }
            })
            .collect(Collectors.toList());

        return arrangeGossipCandidates(gossipCandidates);
    }

    private List<GossipSeed> arrangeGossipCandidates(List<MemberInfoDto> members) {
        List<GossipSeed> managers = new ArrayList<>();
        List<GossipSeed> nodes = new ArrayList<>();

        members.forEach(m -> {
            InetSocketAddress address = new InetSocketAddress(m.externalHttpIp, m.externalHttpPort);

            if (m.state == VNodeState.Manager) {
                managers.add(new GossipSeed(address));
            } else {
                nodes.add(new GossipSeed(address));
            }
        });

        Collections.shuffle(managers);
        Collections.shuffle(nodes);

        List<GossipSeed> result = new ArrayList<>();
        result.addAll(nodes);
        result.addAll(managers);

        return result;
    }

    private Optional<ClusterInfoDto> tryGetGossipFrom(GossipSeed gossipSeed) {

      String url = "https://" + gossipSeed.endpoint.getHostString() + ":" + gossipSeed.endpoint.getPort() + "/gossip?format=json";
      logger.debug("url : {}",url);
      Request request = new Request.Builder()
          .url(url)
          .get()
          .build();

          try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                    return Optional.of(gson.fromJson(new JsonReader(
                        Objects.requireNonNull(response.body()).charStream()), ClusterInfoDto.class));

            }
        } catch (Exception e) {
            // ignore
            logger.warn("get gossip error",e);
        }

        return Optional.empty();
    }

    private Optional<NodeEndpoints> tryDetermineBestNode(List<MemberInfoDto> members) {
        Predicate<VNodeState> matchesNotAllowedStates = s ->
            s == VNodeState.Manager || s == VNodeState.ShuttingDown || s == VNodeState.Shutdown;

        List<MemberInfoDto> aliveMembers = members.stream()
            .filter(m -> m.isAlive && !matchesNotAllowedStates.test(m.state))
            .sorted((a, b) -> a.state.ordinal() > b.state.ordinal() ? -1 : 1)
            .collect(toList());

        switch (settings.nodePreference) {
            case Random:
                Collections.shuffle(aliveMembers);
                break;
            case Slave:
                aliveMembers = aliveMembers.stream()
                    .sorted((a, b) -> a.state == VNodeState.Slave ? -1 : 1)
                    .collect(toList());
                Collections.shuffle(aliveMembers.subList(0, (int) aliveMembers.stream().filter(m -> m.state == VNodeState.Slave).count()));
                break;
        }

        return aliveMembers.stream()
            .findFirst()
            .map(n -> {
                InetSocketAddress tcp = new InetSocketAddress(n.externalTcpIp, n.externalTcpPort);
                InetSocketAddress secureTcp = n.externalSecureTcpPort > 0 ? new InetSocketAddress(n.externalTcpIp, n.externalSecureTcpPort) : null;

                logger.info("Discovering: found best choice [{},{}] ({}).", tcp, secureTcp == null ? "n/a" : secureTcp.toString(), n.state);

                return new NodeEndpoints(tcp, secureTcp);
            });
    }
  private static OkHttpClient getUnsafeOkHttpClient() {
    try {
      // Create a trust manager that does not validate certificate chains
      final TrustManager[] trustAllCerts = new TrustManager[] {
          new X509TrustManager() {
            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
            }

            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return new java.security.cert.X509Certificate[]{};
            }
          }
      };

      // Install the all-trusting trust manager
      final SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      // Create an ssl socket factory with our all-trusting manager
      final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
      builder.hostnameVerifier(new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession session) {
          return true;
        }
      });
      builder.followRedirects(true);
      builder.followSslRedirects(true);
      builder.connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS));


      OkHttpClient okHttpClient = builder.build();
      return okHttpClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

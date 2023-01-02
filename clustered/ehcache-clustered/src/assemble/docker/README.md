## Building Ehcache Docker Images

* Directory `config-tool`: Dockerfile and content for the Terracotta config-tool image (used to build and configure a cluster)
* Directory `server`: Dockerfile and content for the Terracotta Server image
* Directory `voter`: Dockerfile and content for the Terracotta Voter image (used in consistency mode)

### Steps:

1. Start a shell at the root of the Ehcache kit

2. Build images:

```bash
docker build -f docker/server/Dockerfile -t ehcache-terracotta-server:@version@ .
docker build -f docker/config-tool/Dockerfile -t ehcache-terracotta-config-tool:@version@ .
docker build -f docker/voter/Dockerfile -t ehcache-terracotta-voter:@version@ .
```

### Verifying the built images

1. Create a network:

```bash
docker network create terracotta-net
```

2. Run a server interactively in consistency mode:

```bash
docker run -it --rm \
  -e DEFAULT_ACTIVATE="true" \
  -e DEFAULT_FAILOVER="consistency:1" \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  --user 1234:0 \
  ehcache-terracotta-server:@version@
```

3. Try the voter image:

```bash
docker run -it --rm \
  --network terracotta-net \
  --user 1234:0 \
  ehcache-terracotta-voter:@version@ -connect-to ehcache-terracotta-server:9410
```

4. Try the config-tool image:

```bash
docker run -it --rm \
  --network terracotta-net \
  --user 1234:0 \
  ehcache-terracotta-config-tool:@version@ diagnostic -connect-to ehcache-terracotta-server
```

### Automated Way

Run `./docker/buildAndTest.groovy`

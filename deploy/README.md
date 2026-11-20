# Deploying open-connect

- `control-plane/`: two controllers, Postgres and an nginx that terminates TLS
  in front of both controllers.
- `node/`: Strom, the Strom adapter and a `node.yaml` template, for each media
  node.

Both use the image `ghcr.io/kristalalfred/open-connect:latest` unless
`CONNECT_IMAGE` in their `.env` names another. Each `v*` tag publishes the image
for linux/amd64 and linux/arm64 under the version, such as `0.1.0` or
`0.1.0-rc.1`. `latest` and `0.1` follow the newest tag without a pre-release
part, so set `CONNECT_IMAGE` to the version when running a pre-release.

For a local build, run `docker build -t open-connect:local .` at the repository
root and set `CONNECT_IMAGE=open-connect:local` in each kit's `.env`. Build or load
the image on each host that runs a kit.

The image holds `connect`, `connect-controller`, `connect-adapter-strom` and
`connect-node`, runs as uid 10001 and starts under `tini`.

## Control plane

1. Copy `control-plane/env.example` to `control-plane/.env` and fill in the four
   secrets, each from `openssl rand -hex 32`. `CONNECT_SOUTHBOUND_KEY` and
   `CONNECT_SRT_KEY_SECRET` must be at least 32 characters. Every controller needs
   the same values; the compose file gives both the same. Set `CONNECT_IMAGE` as
   described above.
2. Put a certificate and key in `control-plane/tls/` as `server.pem` and
   `server.key`. The certificate has to cover the name nodes and operators dial.
3. `docker compose up -d` in `control-plane/`.

### Ports

| Port | Service | Open to |
|---|---|---|
| 443 | nginx to both controllers' APIs | operators and every node |
| 8090, 8091 | controller dashboards at `/ui` | published on `127.0.0.1` only |

nginx sends each request to either controller, and moves to the other when it
cannot connect within two seconds. The standby forwards to the leader (see
[Forwarding](../docs/operations.md#forwarding)). Each controller advertises its
Compose service name, such as `http://controller-1:8082`. Postgres and the
controllers' API port 8082 are reachable only on the Compose network.

### Minting node tokens

A node's token is derived from `CONNECT_SOUTHBOUND_KEY`, and minting one calls no
service. The controller containers hold the key, so in `control-plane/`:

```sh
docker compose exec controller-1 connect node-token strom-node-1
```

A `connect` outside the container mints the same token with the key in its
`CONNECT_SOUTHBOUND_KEY`. `connect node-token <id> --epoch <n>` mints at another
epoch. Put the token in the node's `CONNECT_SOUTHBOUND_TOKEN`. Adding a node needs
no control-plane restart.

### Talking to it

The image holds `connect`. Each tagged release also has it in its archives, with
the four services (see [Releases](../docs/operations.md#releases)). From a
checkout, `cargo install --locked --path crates/cli` installs it into Cargo's
bin directory.

```sh
export CONNECT_NORTHBOUND_TOKEN=...
connect --url https://<control-plane-host> get nodes
```

`--url` (or `CONNECT_NORTHBOUND_URL`) defaults to `http://127.0.0.1:8082`. When
the certificate in `control-plane/tls/` is signed by a private CA, set
`SSL_CERT_FILE` to the CA as a PEM file (see [TLS](../docs/security.md#tls)).

### The dashboard has no login

The controller serves `/`, `/ui`, `/view` and `/status` without authentication,
and `/view` shows topology and allocated ports. The compose file publishes the controller
ports on `127.0.0.1`. Reach them over an SSH tunnel or a private network, and do
not publish them on a public address.

See [Operations](../docs/operations.md) for configuration and failover, and
[Security](../docs/security.md) for authentication and TLS.

## Node kit

Run on each media node host.

1. Copy `node/node.yaml` and replace every `<placeholder>`. `node.id` must match
   the id the token was minted for. `southbound_url` is the control plane's HTTPS URL.
   The `listeners` addresses are what other nodes dial to reach this node's Strom.
2. Copy `node/env.example` to `node/.env`. Set `CONNECT_SOUTHBOUND_TOKEN` to the
   node's token and `STROM_API_KEY` to the output of `openssl rand -hex 32`.
   Set `CONNECT_IMAGE` as the control plane's `.env` does.
3. `docker compose up -d` in `node/`.

### Strom's API key

Strom listens on every interface, and host networking puts its port 8080 on the
host's addresses. The kit starts Strom with `STROM_API_KEY` and gives the adapter
the same value as `CONNECT_STROM_TOKEN`; it refuses to start without one. With the
key set, Strom's `/api` routes that list, create, start and delete flows require
it, and so does `GET /api/flows`, which returns the SRT link keys. The WHIP and
WHEP routes, `/whip/<id>` and `/whep/<id>`, do not, so outside senders and
players need no key.

Strom runs `eyevinntechnology/strom:0.6.10`, the version the bench runs. Both
containers use host networking, so the SRT port range in `node.yaml` is the
host's own. The host must support Docker host networking.

If the control plane's certificate is signed by a private CA, mount the CA as a PEM file
in the adapter container and set `SSL_CERT_FILE` to it.

### Node ports

| Port | Protocol | Open to |
|---|---|---|
| 20000-20999 | UDP | Other nodes and external peers, the `port_range` in `node.yaml`. Change both together. |
| 8080 | TCP | Only if the node serves WHIP or WHEP. This is also Strom's own API, which requires `STROM_API_KEY` (see [Strom's API key](#stroms-api-key)). |
| 8091 | TCP | The adapter's `/health`. Nothing needs it beyond your own monitoring. |

The node dials out to the control plane on 443.

## First stream

`examples/stream.yaml` names `strom-node-1` and `strom-node-2`, so bring up two
node kits with those ids, then, from the repository root:

```sh
export CONNECT_NORTHBOUND_URL=https://<control-plane-host>
export CONNECT_NORTHBOUND_TOKEN=<the control plane's northbound token>
export SSL_CERT_FILE=<the CA's PEM file>
connect apply -f examples/stream.yaml
connect get endpoints cam1-to-studio
```

Send a test source to the `ingress` URL and play a destination URL:

```sh
ffmpeg -re -f lavfi -i testsrc2=size=640x360:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -g 30 \
  -c:a aac -f mpegts "srt://<ingress host>:<port>?mode=caller"
ffplay "srt://<studio host>:<port>?mode=caller"
```

`connect get stream cam1-to-studio` reads `flowing` once media reaches every
destination. Until then its DESTINATION table names each destination that is
not flowing and why. A destination nothing pulls from reads `degraded` with
`media_degraded`.

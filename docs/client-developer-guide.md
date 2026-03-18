# Ditto Client Libraries – Developer Guide

This guide covers the **Node.js**, **Java**, and **Python** client libraries for dittod.
All libraries support the same two protocols and expose a symmetric API.

---

## Table of Contents

- [Protocol Overview](#protocol-overview)
- [Node.js Client (`ditto-nodejs-client`)](#nodejs-client)
  - [Installation](#nodejs-installation)
  - [TCP Client](#nodejs-tcp-client)
  - [HTTP Client](#nodejs-http-client)
  - [Error Handling](#nodejs-error-handling)
  - [API Reference](#nodejs-api-reference)
- [Java Client (`ditto-java-client`)](#java-client)
  - [Build & Dependency](#java-build--dependency)
  - [TCP Client](#java-tcp-client)
  - [HTTP Client](#java-http-client)
  - [Error Handling](#java-error-handling)
  - [API Reference](#java-api-reference)
- [Python Client (`ditto-python-client`)](#python-client)
  - [Installation](#python-installation)
  - [TCP Client](#python-tcp-client)
  - [HTTP Client](#python-http-client)
  - [Error Handling](#python-error-handling)
  - [API Reference](#python-api-reference)
- [HTTP API YAML – Code Generation](#http-api-yaml--code-generation)
  - [Adding or Modifying an Endpoint](#adding-or-modifying-an-endpoint)
  - [Removing an Endpoint](#removing-an-endpoint)
  - [Version Bump Rules](#version-bump-rules)
- [Docker Test Environments](#docker-test-environments)
  - [Node.js Tests](#nodejs-tests)
  - [Java Tests](#java-tests)
  - [Python Tests](#python-tests)

---

## Protocol Overview

Each dittod node exposes two client-facing protocols:

| Protocol | Port | Description |
|----------|------|-------------|
| **TCP binary** | 7777 | bincode 1.x framed messages; lowest latency |
| **HTTP REST**  | 7778 | plain JSON; easiest to integrate |

Both protocols expose the same operations: `ping`, `get`, `set`, `delete`.
The HTTP protocol additionally exposes `stats`.

```
App ──TCP bincode──▶ dittod :7777   (DittoTcpClient)
App ──HTTP REST────▶ dittod :7778   (DittoHttpClient)
```

TCP wire framing: every message is preceded by a **4-byte big-endian payload length**.
The payload is serialized with [bincode 1.x](https://github.com/bincode-org/bincode)
(little-endian integers, `u64 LE` length-prefixed strings, `u8` option tag).

---

## Node.js Client

### Node.js Installation

```bash
# from the repo root
cd src/ditto-nodejs-client
npm install
npm run build       # compiles TypeScript → dist/
```

Or reference the local package from another project:

```json
{
  "dependencies": {
    "ditto-client": "file:../path/to/ditto-nodejs-client"
  }
}
```

Requires **Node.js ≥ 22**.

---

### Node.js TCP Client

`DittoTcpClient` connects to dittod port **7777** over a persistent TCP socket.
Requests are serialized (one in-flight at a time via a pending queue).

```typescript
import { DittoTcpClient } from 'ditto-client';

const client = new DittoTcpClient({ host: 'localhost', port: 7777, authToken: 'my-secret-token' });
await client.connect();

// ping
const alive = await client.ping();           // true

// set with TTL (300 seconds)
const { version } = await client.set('session:abc', 'tok_xyz', 300);

// set without TTL (Buffer or string)
await client.set('session:xyz', Buffer.from('tok_abc'));

// get  →  DittoGetResult | null
const result = await client.get('session:abc');
if (result) {
  console.log(result.value.toString('utf8')); // 'tok_xyz'
  console.log(result.version);               // 1
}

// delete  →  true (existed) | false (not found)
const removed = await client.delete('session:abc');

await client.close();
```

---

### Node.js HTTP Client

`DittoHttpClient` connects to dittod port **7778** via HTTP REST.
Uses the native `fetch` API (no external HTTP dependency).

> The implementation is **generated** from `src/api/ditto-http-api.yaml`.
> See [HTTP API YAML – Code Generation](#http-api-yaml--code-generation).

```typescript
import { DittoHttpClient } from 'ditto-client';

const client = new DittoHttpClient({ host: 'localhost', port: 7778 });

await client.ping();                         // true

const { version } = await client.set('user:1', 'Alice', 60);

const result = await client.get('user:1');   // DittoGetResult | null
console.log(result?.value.toString());       // 'Alice'

const stats = await client.stats();
console.log(stats.key_count, stats.memory_used_bytes);

await client.delete('user:1');

client.close(); // no-op (HTTP is stateless)
```

**HTTPS / Basic Auth:**

```typescript
const client = new DittoHttpClient({
  host: 'my-node.example.com',
  port: 7778,
  tls:  true,
  username: 'admin',
  password: 'secret',
  rejectUnauthorized: false,  // allow self-signed certs
});
```

---

### Node.js Error Handling

All errors from the server are thrown as `DittoError`:

```typescript
import { DittoHttpClient, DittoError } from 'ditto-client';

try {
  await client.set('key', 'value');
} catch (err) {
  if (err instanceof DittoError) {
    console.error(err.code);    // e.g. 'NodeInactive' | 'KeyLimitReached'
    console.error(err.message); // human-readable description
  }
}
```

**Error codes:**

| Code | Meaning |
|------|---------|
| `NodeInactive` | Node is not in the active set (not yet elected primary) |
| `NoQuorum` | Quorum lost; writes rejected |
| `KeyNotFound` | Requested key does not exist |
| `InternalError` | Unspecified server error |
| `WriteTimeout` | Write did not replicate within the timeout |
| `ValueTooLarge` | Value exceeds the server's size limit |
| `KeyLimitReached` | The maximum number of keys has been reached |

---

### Node.js API Reference

#### `DittoTcpClient`

```typescript
constructor(opts?: { host?: string; port?: number; authToken?: string })
connect(): Promise<void>
ping():                                   Promise<boolean>
get(key: string):                         Promise<DittoGetResult | null>
set(key: string, value: string|Buffer, ttlSecs?: number): Promise<DittoSetResult>
delete(key: string):                      Promise<boolean>
close(): Promise<void>
```

#### `DittoHttpClient`

```typescript
constructor(opts?: DittoHttpClientOptions)
ping():                                           Promise<boolean>
get(key: string):                                 Promise<DittoGetResult | null>
set(key: string, value: string, ttlSecs?: number):Promise<DittoSetResult>
delete(key: string):                              Promise<boolean>
stats():                                          Promise<DittoStatsResult>
close(): void
```

#### Result types

```typescript
interface DittoGetResult  { value: Buffer; version: number }
interface DittoSetResult  { version: number }
interface DittoStatsResult {
  node_id: string; status: string; is_primary: boolean;
  key_count: number; memory_used_bytes: number; memory_max_bytes: number;
  hit_count: number; miss_count: number; uptime_secs: number;
  // ... (see types.ts for full definition)
}
```

---

## Java Client

Both `DittoTcpClient` and `DittoHttpClient` are in the `io.ditto.client` package.

### Java Build & Dependency

```bash
cd src/ditto-java-client
gradle jar          # produces build/libs/ditto-java-client.jar
```

Requires **Java 21** and **Gradle 8+**.

Declare as a local file dependency in another project's `build.gradle.kts`:

```kotlin
dependencies {
    implementation(files("path/to/ditto-java-client.jar"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2") // required transitive
}
```

---

### Java TCP Client

`DittoTcpClient` connects to dittod port **7777**.
All public methods are `synchronized`, so concurrent calls from multiple threads
are safely serialized over the single socket.

```java
import io.ditto.client.*;

try (DittoTcpClient client = new DittoTcpClient("localhost", 7777, "my-secret-token")) {
    client.connect();

    boolean alive = client.ping();                   // true

    // set with TTL (string value)
    DittoSetResult r1 = client.set("session:abc", "tok_xyz", 300);

    // set without TTL (byte[] value)
    DittoSetResult r2 = client.set("session:xyz", "tok_abc".getBytes());

    // get  →  DittoGetResult | null
    DittoGetResult result = client.get("session:abc");
    if (result != null) {
        System.out.println(result.getValueAsString()); // "tok_xyz"
        System.out.println(result.getVersion());       // 1
    }

    // delete  →  true (existed) | false (not found)
    boolean removed = client.delete("session:abc");
}
// socket closed automatically via try-with-resources
```

---

### Java HTTP Client

`DittoHttpClient` connects to dittod port **7778**.
Uses the JDK built-in `HttpClient` (Java 11+).

> The endpoint methods are **generated** from `src/api/ditto-http-api.yaml`.
> See [HTTP API YAML – Code Generation](#http-api-yaml--code-generation).

```java
import io.ditto.client.*;

DittoHttpClient client = DittoHttpClient.builder()
    .host("localhost")
    .port(7778)
    .build();

client.ping();                                    // true

DittoSetResult r = client.set("user:1", "Alice", 60);

DittoGetResult result = client.get("user:1");     // null if not found
System.out.println(result.getValueAsString());    // "Alice"

DittoStatsResult stats = client.stats();
System.out.println(stats.getKeyCount());

client.delete("user:1");
client.close();
```

**Static factory shortcuts:**

```java
DittoHttpClient c1 = DittoHttpClient.connect();                  // localhost:7778
DittoHttpClient c2 = DittoHttpClient.connect("my-node");         // my-node:7778
DittoHttpClient c3 = DittoHttpClient.connect("my-node", 7778);
```

**HTTPS / Basic Auth:**

```java
DittoHttpClient client = DittoHttpClient.builder()
    .host("my-node.example.com")
    .port(7778)
    .tls(true)
    .username("admin")
    .password("secret")
    .build();
```

---

### Java Error Handling

Server errors are thrown as `DittoException` (a `RuntimeException`):

```java
try {
    client.set("key", "value");
} catch (DittoException ex) {
    System.err.println(ex.getCode());    // DittoErrorCode enum value
    System.err.println(ex.getMessage()); // human-readable description
}
```

`DittoErrorCode` values mirror the Node.js codes exactly:
`NODE_INACTIVE`, `NO_QUORUM`, `KEY_NOT_FOUND`, `INTERNAL_ERROR`,
`WRITE_TIMEOUT`, `VALUE_TOO_LARGE`, `KEY_LIMIT_REACHED`.

---

### Java API Reference

#### `DittoTcpClient`

```java
DittoTcpClient(String host, int port)
DittoTcpClient(String host, int port, String authToken)
DittoTcpClient()                                     // localhost:7777
void    connect()                        throws IOException
boolean ping()                           throws IOException
DittoGetResult  get(String key)          throws IOException
DittoSetResult  set(String key, String value)                      throws IOException
DittoSetResult  set(String key, String value, long ttlSecs)        throws IOException
DittoSetResult  set(String key, byte[] value)                      throws IOException
DittoSetResult  set(String key, byte[] value, long ttlSecs)        throws IOException
boolean delete(String key)               throws IOException
void    close()                          throws IOException  // also AutoCloseable
```

#### `DittoHttpClient`

```java
// factory
static Builder              builder()
static DittoHttpClient      connect()
static DittoHttpClient      connect(String host)
static DittoHttpClient      connect(String host, int port)

// endpoint methods (generated)
boolean         ping()                                    throws IOException, InterruptedException
DittoGetResult  get(String key)                           throws IOException, InterruptedException
DittoSetResult  set(String key, String value)             throws IOException, InterruptedException
DittoSetResult  set(String key, String value, long ttlSecs) throws IOException, InterruptedException
boolean         delete(String key)                        throws IOException, InterruptedException
DittoStatsResult stats()                                  throws IOException, InterruptedException
void            close()
```

---

## Python Client

Both `DittoTcpClient` and `DittoHttpClient` are in the `ditto_client` package.
Requires **Python ≥ 3.11**. Zero external dependencies (stdlib only).

### Python Installation

```bash
# from the repo root
cd src/ditto-python-client
pip install -e .
```

Or reference the local package from another project:

```bash
pip install "ditto-client @ file:///path/to/ditto-python-client"
```

---

### Python TCP Client

`DittoTcpClient` connects to dittod port **7777** over a persistent TCP socket.
Requests are serialized via an internal threading lock — safe to call from multiple threads.

```python
from ditto_client import DittoTcpClient

# As a context manager (automatically calls connect() / close())
with DittoTcpClient(host='localhost', port=7777, auth_token='my-secret-token') as client:

    alive = client.ping()                              # True

    r = client.set('session:abc', 'tok_xyz', ttl_secs=300)
    print(r.version)                                   # 1

    # bytes value, no TTL
    client.set('session:xyz', b'tok_abc')

    result = client.get('session:abc')
    if result:
        print(result.value.decode())                   # 'tok_xyz'
        print(result.version)                          # 1

    removed = client.delete('session:abc')             # True

# Or manually:
client = DittoTcpClient()
client.connect()
client.ping()
client.close()
```

---

### Python HTTP Client

`DittoHttpClient` connects to dittod port **7778** via HTTP REST.
Uses the stdlib `urllib.request` — no external HTTP dependency.

> The implementation is **generated** from `src/api/ditto-http-api.yaml`.
> See [HTTP API YAML – Code Generation](#http-api-yaml--code-generation).

```python
from ditto_client import DittoHttpClient

client = DittoHttpClient(host='localhost', port=7778)

client.ping()                                          # True

r = client.set('user:1', 'Alice', ttl_secs=60)
print(r.version)                                       # 1

result = client.get('user:1')                          # DittoGetResult | None
print(result.value.decode())                           # 'Alice'

stats = client.stats()
print(stats.key_count, stats.memory_used_bytes)

client.delete('user:1')
client.close()
```

**HTTPS / Basic Auth:**

```python
client = DittoHttpClient(
    host='my-node.example.com',
    port=7778,
    tls=True,
    username='admin',
    password='secret',
    reject_unauthorized=False,   # allow self-signed certs
)
```

---

### Python Error Handling

All errors from the server are raised as `DittoError`:

```python
from ditto_client import DittoHttpClient, DittoError, DittoErrorCode

try:
    client.set('key', 'value')
except DittoError as exc:
    print(exc.code)     # e.g. DittoErrorCode.NODE_INACTIVE
    print(str(exc))     # human-readable description
```

**Error codes** (`DittoErrorCode` enum):

| Enum value | String | Meaning |
|------------|--------|---------|
| `NODE_INACTIVE` | `NodeInactive` | Node is not in the active set |
| `NO_QUORUM` | `NoQuorum` | Quorum lost; writes rejected |
| `KEY_NOT_FOUND` | `KeyNotFound` | Requested key does not exist |
| `INTERNAL_ERROR` | `InternalError` | Unspecified server error |
| `WRITE_TIMEOUT` | `WriteTimeout` | Write did not replicate within the timeout |
| `VALUE_TOO_LARGE` | `ValueTooLarge` | Value exceeds the server's size limit |
| `KEY_LIMIT_REACHED` | `KeyLimitReached` | Maximum number of keys has been reached |

---

### Python API Reference

#### `DittoTcpClient`

```python
DittoTcpClient(host: str = 'localhost', port: int = 7777, auth_token: str | None = None)
connect() -> None
ping()                                         -> bool
get(key: str)                                  -> DittoGetResult | None
set(key: str, value: str | bytes, ttl_secs: int = 0) -> DittoSetResult
delete(key: str)                               -> bool
close()  -> None
# Also usable as a context manager (with statement).
```

#### `DittoHttpClient`

```python
DittoHttpClient(host='localhost', port=7778, *, tls=False,
                username=None, password=None, reject_unauthorized=True)
ping()                                         -> bool
get(key: str)                                  -> DittoGetResult | None
set(key: str, value: str, ttl_secs: int = 0)  -> DittoSetResult
delete(key: str)                               -> bool
stats()                                        -> DittoStatsResult
close() -> None
```

#### Result types

```python
@dataclass(frozen=True)
class DittoGetResult:
    value:   bytes
    version: int

@dataclass(frozen=True)
class DittoSetResult:
    version: int

@dataclass
class DittoStatsResult:
    node_id: str; status: str; is_primary: bool
    key_count: int; memory_used_bytes: int; memory_max_bytes: int
    hit_count: int; miss_count: int; uptime_secs: int
    # ... (see types.py for full definition)
```

---

## HTTP API YAML – Code Generation

The file `src/api/ditto-http-api.yaml` is the **single source of truth** for the
HTTP REST interface. All three clients' HTTP implementations are generated from it.

### File structure

```
src/
├── api/
│   └── ditto-http-api.yaml              ← edit this to change the HTTP API
├── tools/
│   ├── generate-http-clients.mjs        ← generator script
│   └── package.json
├── ditto-nodejs-client/src/
│   ├── http-client-base.ts              ← manual: constructor, request, assertOk
│   └── http-client.ts                   ← GENERATED — do not edit manually
├── ditto-java-client/src/.../
│   ├── DittoHttpClientBase.java         ← manual: constructor, send, assertOk
│   └── DittoHttpClient.java             ← GENERATED — do not edit manually
└── ditto-python-client/src/ditto_client/
    ├── http_client_base.py              ← manual: constructor, _request, _assert_ok
    └── http_client.py                   ← GENERATED — do not edit manually
```

### Regenerating the clients

```bash
cd src/tools
npm install        # first time only
npm run generate
```

Output:

```
generating from api/ditto-http-api.yaml  v1.2.0

  ✓  wrote src/ditto-nodejs-client/src/http-client.ts
  ✓  wrote src/ditto-java-client/src/main/java/io/ditto/client/DittoHttpClient.java
  ✓  wrote src/ditto-python-client/src/ditto_client/http_client.py

done.
```

### Adding or Modifying an Endpoint

1. Open `src/api/ditto-http-api.yaml`.

2. Add (or edit) a path entry following the existing pattern.
   Each operation requires an `x-ditto` extension block:

   ```yaml
   /flush:
     post:
       operationId: flush
       summary: Flush all keys from the cache immediately.
       x-ditto:
         ts:
           return: "Promise<void>"
           body: |
             const resp = await this.request('/flush', { method: 'POST' });
             await this.assertOk(resp);
         java:
           return: "void"
           body: |
             HttpResponse<String> resp = send(requestBuilder("/flush")
                     .POST(HttpRequest.BodyPublishers.noBody()).build());
             assertOk(resp);
         py:
           return: "None"
           body: |
             status, body = self._request('/flush', method='POST')
             self._assert_ok(status, body)
       responses:
         "204":
           description: Cache flushed
   ```

   **`x-ditto` fields:**

   | Field | Required | Description |
   |-------|----------|-------------|
   | `ts.return` | ✓ | Full TypeScript return type (e.g. `Promise<boolean>`) |
   | `ts.params` | – | TypeScript parameter list (omit for no params) |
   | `ts.body` | ✓ | TypeScript method body (literal block `\|`) |
   | `java.return` | ✓ | Java return type (e.g. `boolean`, `DittoGetResult`) |
   | `java.params` | – | Java parameter list for the main overload |
   | `java.body` | ✓ | Java method body (literal block `\|`) |
   | `java.overloads` | – | List of additional overloads (e.g. no-TTL convenience method) |
   | `py.return` | ✓ | Python return annotation (e.g. `bool`, `DittoGetResult \| None`) |
   | `py.params` | – | Python parameter list without `self` (omit for no params) |
   | `py.body` | ✓ | Python method body (literal block `\|`) |

3. Bump `info.version` following [the rules below](#version-bump-rules).

4. Run the generator:
   ```bash
   cd src/tools && npm run generate
   ```

5. Rebuild and re-run the Docker tests to verify.

### Removing an Endpoint

1. Delete the path (or HTTP method entry) from `ditto-http-api.yaml`.
2. Bump `info.version`.
3. Run the generator — the method disappears from both clients automatically.

### Version Bump Rules

| Change type | Version part to bump |
|-------------|----------------------|
| Implementation fix (no API change) | patch (`1.1.0` → `1.1.1`) |
| New endpoint added | minor (`1.1.0` → `1.2.0`) |
| Endpoint removed or signature changed | major (`1.1.0` → `2.0.0`) |

The version appears as a comment header in both generated files, making it easy
to check which spec version a build was generated from.

---

## Docker Test Environments

Both test environments start a single dittod node (no TLS, no auth, no cluster)
and two test containers that run against it.

### Node.js Tests

```bash
cd src/docker/clients/nodejs
docker compose up --build --abort-on-container-exit
```

| Container | Tests |
|-----------|-------|
| `test-http` | ping, set/TTL, get, stats, delete, DittoError |
| `test-tcp`  | ping, set (string + Buffer), get, delete, rapid sequential, DittoError |

Expected output:

```
ditto-test-http | [HTTP] All tests passed.
ditto-test-tcp  | [TCP] All tests passed.
```

### Java Tests

```bash
cd src/docker/clients/java
docker compose up --build --abort-on-container-exit
```

| Container | Tests |
|-----------|-------|
| `test-http` | ping, set/TTL, get, stats, delete, DittoException |
| `test-tcp`  | ping, set (String + byte[]), get, delete, sequential, DittoException |

Expected output:

```
ditto-java-test-http | [HTTP] All tests passed.
ditto-java-test-tcp  | [TCP] All tests passed.
```

### Python Tests

```bash
cd src/docker/clients/python
docker compose up --build --abort-on-container-exit
```

| Container | Tests |
|-----------|-------|
| `test-http` | ping, set/TTL, get, stats, delete, DittoError |
| `test-tcp`  | ping, set (str + bytes), get, delete, rapid sequential, DittoError |

Expected output:

```
ditto-python-test-http | [HTTP] All tests passed.
ditto-python-test-tcp  | [TCP] All tests passed.
```

### Troubleshooting

**Port already in use**
The compose files do **not** map host ports — dittod is only reachable inside
the Docker network. If you see a port conflict it is from a previously running
dittod container; stop it with:
```bash
docker ps -a | grep ditto
docker rm -f <container-id>
```

**`src` path not found**
The build context for all test images is `src/` (three levels up from
`docker/clients/*/`). Make sure you run `docker compose up` from the correct
directory (`src/docker/clients/nodejs/` or `src/docker/clients/java/`).

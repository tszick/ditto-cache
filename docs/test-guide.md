# Ditto – Teljes tesztelési kézikönyv

## Port-térkép (docker-compose)

| Node   | client TCP | HTTP  | cluster/admin (mTLS) | gossip UDP |
|--------|-----------|-------|----------------------|------------|
| node-1 | 7777      | 7778  | 7779                 | 7780       |
| node-2 | 7787      | 7788  | **7789**             | 7790       |
| node-3 | 7797      | 7798  | **7799**             | 7800       |

> **dittoctl célpontok:** `local` = node-1 (127.0.0.1:7779), `all` = összes seed,
> node-2 = `127.0.0.1:7789`, node-3 = `127.0.0.1:7799`
>
> **curl:** node-1 = `http://localhost:7778`, node-2 = `http://localhost:7788`, node-3 = `http://localhost:7798`

---

## 0. Előkészítés

```powershell
# Certifikátok generálása (egyszer, Docker Desktop shellben)
apk add openssl
sh docker/gen-certs.sh

# Cluster indítás
docker compose -f docker/docker-compose.yml up --build -d
docker ps   # 3 container kell: ditto-node-1, ditto-node-2, ditto-node-3

# dittoctl config másolása
mkdir "$env:APPDATA\ditto" -Force
Copy-Item .\dittoctl\kvctl.toml "$env:APPDATA\ditto\kvctl.toml" -Force
# Szerkesztd meg: tls.enabled=true, ca/cert/key path-ok kitöltve

# Alias (opcionális)
Set-Alias ditto .\target\release\dittoctl.exe

# Multi-node: seeds frissítése (hogy "all" target mindhárom node-ot elérje)
ditto node set seeds local "127.0.0.1:7779,127.0.0.1:7789,127.0.0.1:7799"
```

---

## 1. Smoke tesztek

```powershell
# Cluster áttekintés
ditto cluster list nodes
# Várt output: 3 sor, mind Active

ditto cluster get status
# Várt: total: 3, active: 3

ditto cluster get primary
# Várt: az UUID-alapú auto-primary (legkisebb UUID)

# HTTP ping
curl http://localhost:7778/ping   # {"pong":true}
curl http://localhost:7788/ping
curl http://localhost:7798/ping
```

---

## 2. Node describe / get / list

```powershell
# Összes tulajdonság egy node-on
ditto node describe local
# Tartalmazza: id, status, primary, client-port, http-port, cluster-port, gossip-port,
#              max-memory, default-ttl, committed-index, uptime,
#              value-size-limit, max-keys, compression-enabled, compression-threshold

# Összes node egyszerre
ditto node describe all

# Egyedi property lekérés
ditto node get status local               # → Active
ditto node get primary local              # → true / false
ditto node get committed-index local      # → 0
ditto node get uptime local               # → Xs
ditto node get compression-enabled local  # → true
ditto node get compression-threshold local# → 4096b
ditto node get value-size-limit local     # → 104857600b (100 MB alapértelmezett)
ditto node get max-keys local             # → 100000 (vagy unlimited ha 0)
ditto node get max-memory local           # → 512mb

# Port lista
ditto node list ports local
# Sorok: client-port, http-port, cluster-port, gossip-port

# HTTP stats endpoint (minden NodeStats mezőt tartalmaz JSON-ban)
curl http://localhost:7778/stats
# JSON: node_id, status, is_primary, committed_index, key_count,
#       memory_used_bytes, memory_max_bytes, evictions, hit_count, miss_count,
#       uptime_secs, value_size_limit_bytes, max_keys_limit,
#       compression_enabled, compression_threshold_bytes
```

---

## 3. Cache – alapműveletek

### 3.1 Set / Get / Delete

```powershell
# Set (bináris TCP protokoll – dittoctl)
ditto cache set local foo bar
ditto cache set local foo2 "hello world" --ttl 60

# HTTP Set
curl -X PUT http://localhost:7778/key/foo -d "bar"
curl -X PUT "http://localhost:7778/key/foo2?ttl=60" -d "hello world"

# Get (részletes: key, value, version, freq, compressed, ttl)
ditto cache get key local foo

# HTTP Get
curl http://localhost:7778/key/foo
# JSON: {"value": "bar", "version": 1}

# Get csak TTL
ditto cache get ttl local foo2

# Delete
ditto cache delete local foo
curl -X DELETE http://localhost:7778/key/foo2

# Nem létező kulcs
ditto cache get key local nemletezik      # → NotFound
curl http://localhost:7778/key/nemletezik   # → 404
```

### 3.2 List keys + stats

```powershell
# Összes kulcs
ditto cache list keys local
ditto cache list keys all

# Mintára szűrve
ditto cache list keys local --pattern "foo*"

# Cache stats (hit/miss/eviction/memory)
ditto cache list stats local
```

### 3.3 Flush

```powershell
# Egyetlen node flush
ditto cache flush local

# Összes node flush (megerősítés kell interaktívan)
ditto cache flush all
```

---

## 4. TTL tesztek

```powershell
# Rövid TTL beállítás
ditto cache set local ttltest "hamarlejár" --ttl 5
ditto cache get ttl local ttltest   # ~5s
ditto cache get key local ttltest   # ttl: ~5s

# 6 másodperc várakozás, majd:
Start-Sleep 6
ditto cache get key local ttltest   # → NotFound (TTL sweep eltávolította)

# HTTP-n is:
curl "http://localhost:7778/key/ttltest?ttl=5" -X PUT -d "lejár"
Start-Sleep 6
curl http://localhost:7778/key/ttltest   # → 404

# Default TTL beállítás (minden új kulcsra vonatkozik)
ditto node set default-ttl local 30
ditto cache set local autottl "teszt"   # automatikusan 30s TTL-t kap
ditto cache get ttl local autottl       # ~30s

# Default TTL törlése
ditto node set default-ttl local 0   # 0 = nincs default TTL
```

---

## 5. LZ4 tömörítés

### 5.1 Automatikus tömörítés (threshold felett)

```powershell
# 8 KB adat – threshold (4096b) felett → automatikusan tömörített
$bigval = "x" * 8192
ditto cache set local bigkey $bigval
ditto cache get key local bigkey
# compressed: true

# Kis adat – threshold alatt → nem tömörített
ditto cache set local smallkey "hello"
ditto cache get key local smallkey
# compressed: false

# HTTP-n (curl):
curl -X PUT http://localhost:7778/key/curlbig -d $("y" * 8192)
ditto cache get key local curlbig   # compressed: true
```

### 5.2 Compression kapcsolók

```powershell
# Tömörítés kikapcsolása
ditto node set compression-enabled local false
ditto node get compression-enabled local   # → false

# Új nagy írás: nem lesz tömörítve
ditto cache set local afterdisable ($("x" * 8192))
ditto cache get key local afterdisable   # compressed: false

# Visszakapcsolás
ditto node set compression-enabled local true

# Threshold növelés (csak növelhető, minimum 4096b)
ditto node set compression-threshold local 16384
ditto node get compression-threshold local   # → 16384b

# 5000 byte adat – régi threshold felett, új alatt → most már NEM tömörített
ditto cache set local midval ($("z" * 5000))
ditto cache get key local midval   # compressed: false

# Threshold csökkentési kísérlet → hiba/warning (nem kerül alkalmazásra)
ditto node set compression-threshold local 1024
ditto node get compression-threshold local   # → 16384b (változatlan)
```

### 5.3 Per-key compressed flag kézi állítás

```powershell
# Meglévő kis kulcs kézzel tömörítése
ditto cache set-compressed local smallkey true
ditto cache get key local smallkey   # compressed: true

# Visszaállítás kitömörítve
ditto cache set-compressed local smallkey false
ditto cache get key local smallkey   # compressed: false
```

---

## 6. Value-size-limit

```powershell
# Limit beállítás: 1 KB
ditto node set value-size-limit local 1024
ditto node get value-size-limit local   # → 1024b

# Limit alatti érték → sikeres
ditto cache set local kisadat "hello"   # OK
curl -X PUT http://localhost:7778/key/kisadat -d "hello"   # 200 OK

# Limit feletti érték → hiba
$toobig = "x" * 2000
ditto cache set local nagyadat $toobig
# → Error: ValueTooLarge

curl -X PUT http://localhost:7778/key/nagyadat -d ($("x" * 2000))
# → 400 / error: ValueTooLarge

# Limit törlése (0 = korlátlan)
ditto node set value-size-limit local 0
ditto node get value-size-limit local   # → unlimited
```

---

## 7. Max-keys limit

```powershell
# Limit beállítás: 3 kulcs
ditto node set max-keys local 3

# 3 kulcs beírása → OK
ditto cache set local k1 v1
ditto cache set local k2 v2
ditto cache set local k3 v3

# 4. új kulcs beírása → hiba
ditto cache set local k4 v4
# → Error: KeyLimitReached

curl -X PUT http://localhost:7778/key/k4 -d "v4"
# → KeyLimitReached

# Meglévő kulcs frissítése (nem új) → OK
ditto cache set local k1 "frissített"

# Kulcs törlése után írható új
ditto cache delete local k1
ditto cache set local k4 v4   # OK

# Limit törlése (0 = korlátlan)
ditto node set max-keys local 0
ditto node get max-keys local   # → unlimited
```

---

## 8. Max-memory + LFU eviction

```powershell
# Stats előtte (evictions: 0)
ditto cache list stats local

# Memory limit csökkentés (1 MB)
ditto node set max-memory local 1

# 100 kulcs beírása (néhány KB-os értékekkel) → eviction triggerelés
1..100 | ForEach-Object {
    ditto cache set local "ev$_" ("val" * 100)
}

# Stats utána (evictions > 0)
ditto cache list stats local

# LFU: a ritkán olvasott kulcsok esnek ki először
# Érd el ugyanazt a kulcsot többször, hogy megvédd az evikciótól:
ditto cache get key local ev50
ditto cache get key local ev50
ditto cache get key local ev50

# Memory limit visszaállítás
ditto node set max-memory local 512
```

---

## 9. Port módosítás (Inactive állapot szükséges)

```powershell
# Port módosítása csak Inactive állapotban lehetséges
# Aktív node-on a szerver hibát ad vissza

# 1. Node inaktívvá tétele
ditto node set status local false
ditto node get status local         # → Inactive

# 2. HTTP port módosítása
ditto node set http-port local 7900
ditto node get http-port local      # → 7900 (tervezett, még nem érvényes)

# Ha aktív node-on próbálunk portot módosítani → hiba
ditto node set status local true
ditto node set http-port local 7901
# → Error: port changes require the node to be Inactive

# Valódi port csere (docker container restart):
# 1. ditto node set status local false
# 2. ditto node set http-port local 7900
# 3. docker compose restart ditto-node-1
# 4. curl http://localhost:7900/ping → OK
# 5. curl http://localhost:7778/ping → Connection refused
```

---

## 10. Node status (Active / Inactive)

```powershell
# Node inaktívvá tétele
ditto node set status local false
ditto node get status local   # → Inactive

# Inaktív node elutasítja a klienseket
curl http://localhost:7778/key/foo       # → 503 ServiceUnavailable
ditto cache get key local foo            # → Error: NodeInactive
curl -X PUT http://localhost:7778/key/x -d "y"  # → 503

# Admin parancsok még működnek (a cluster porton)
ditto node describe local   # OK (admin port elérhető)

# Visszaaktiválás
ditto node set status local true
ditto node get status local   # → Active
curl http://localhost:7778/key/foo   # → normál válasz
```

---

## 11. Primary election

### 11.1 Automatikus választás

```powershell
# Az Active node-ok közül a legkisebb UUID a primary
ditto cluster get primary        # UUID stringet ad vissza
curl http://localhost:7778/stats # is_primary: true
curl http://localhost:7788/stats # is_primary: false
curl http://localhost:7798/stats # is_primary: false
```

### 11.2 Force-elect

```powershell
# Node-2 force-elect primaryvá (cluster portja: 7789)
ditto node set primary 127.0.0.1:7789 true

# Ellenőrzés – node-2 UUID jelenik meg
ditto cluster get primary           # → node-2 UUID
curl http://localhost:7788/stats    # is_primary: true
curl http://localhost:7778/stats    # is_primary: false

# Write node-1-re → automatikusan forward node-2-re (primary)
ditto cache set local forwtest "adat"   # OK
curl -X PUT http://localhost:7778/key/fwd -d "http-fwd"  # OK

# Force-elect visszavonása → UUID-alapú auto-elekció
ditto node set primary 127.0.0.1:7789 false
ditto cluster get primary   # → legkisebb UUID
```

### 11.3 Primary failover

```powershell
# Jelenlegi primary inaktiválása
$primaryPort = "127.0.0.1:7779"   # cseréld a tényleges primary portjára
ditto node set status $primaryPort false

# Rövid várakozás után: automatikus új primary választás
Start-Sleep 3
ditto cluster get primary   # → más node UUID
ditto cluster list nodes     # az inaktív node: Inactive, új primary: Active

# Eredeti primary visszaaktiválása
ditto node set status $primaryPort true
ditto cluster list nodes   # mindenki Active
```

---

## 12. Quorum és replikáció

```powershell
# Write replikálódik mindhárom node-ra
ditto cache set local repltest "value1"

# Ellenőrzés minden node-on (HTTP-n)
curl http://localhost:7778/key/repltest   # → "value1"
curl http://localhost:7788/key/repltest   # → "value1"
curl http://localhost:7798/key/repltest   # → "value1"

# Committed index szinkronban van
ditto node get committed-index local
ditto node get committed-index 127.0.0.1:7789
ditto node get committed-index 127.0.0.1:7799
# Mindháromnak azonosnak (vagy max 1-2 eltérés) kell lennie

# Recovery teszt: node-2 ki + write + visszajön
ditto node set status 127.0.0.1:7789 false
ditto cache set local rectest "közben írt"   # quorum: node-1 + node-3

ditto node set status 127.0.0.1:7789 true
# node-2 Syncing → Active (pár másodperc)
ditto cluster list nodes   # node-2: Active

# node-2 is megkapja a közben írt értéket
curl http://localhost:7788/key/rectest   # → "közben írt"
```

---

## 13. NoQuorum teszt

```powershell
# 2 node inaktiválása → 3-ból 1 aktív → nincs quorum (majority: 2/3 kell)
ditto node set status 127.0.0.1:7789 false
ditto node set status 127.0.0.1:7799 false

# Write kísérlet → NoQuorum hiba
curl -X PUT http://localhost:7778/key/noquorum -d "x"
# → 503, error: NoQuorum

ditto cache set local noquorum "x"
# → Error: NoQuorum

# Read az aktív node saját másolatából még működhet
curl http://localhost:7778/key/repltest   # → OK (ha adat ott van)

# Visszaaktiválás
ditto node set status 127.0.0.1:7789 true
ditto node set status 127.0.0.1:7799 true
ditto cluster list nodes   # 3 Active
```

---

## 15. mTLS tesztek

```powershell
# Helyes certekkel (kvctl.toml: tls.enabled = true)
ditto cluster list nodes   # → OK, 3 Active node

# TLS disabled kísérlet (plain TCP → TLS szerver)
# 1. Szerkeszd: "$env:APPDATA\ditto\kvctl.toml" → tls.enabled = false
ditto cluster list nodes
# → Error: early eof VAGY TLS corrupt message

# Server-oldali TLS hibák logban
docker logs ditto-node-1 2>&1 | Select-String "TLS"
# → "TLS accept error: received corrupt message of type InvalidContentType"

# A cluster/admin port (7779/7789/7799) kötelezően mTLS-es.
# A HTTP REST port (7778/7788/7798) alapértelmezésben plaintext HTTP,
# de opcionálisan HTTPS-re konfigurálható — lásd az admin-guide.md-t.

# Visszaállítás: tls.enabled = true a kvctl.toml-ban
```

---

## 16. HTTP Basic Auth tesztek

> **Előfeltétel:** a docker-compose.yml node-okban kommentből kivéve:
> ```yaml
> - DITTO_HTTP_AUTH_USER=ditto
> - DITTO_HTTP_AUTH_PASSWORD_HASH=$2b$12$...   # generálva: dittoctl hash-password
> ```
> és a mgmt service-ben:
> ```yaml
> - DITTO_MGMT_ADMIN_USER=admin
> - DITTO_MGMT_ADMIN_PASSWORD_HASH=$2b$12$...
> - DITTO_MGMT_HTTP_AUTH_USER=ditto
> - DITTO_MGMT_HTTP_AUTH_PASSWORD=mysecret
> ```

```powershell
# --- 7778 (dittod HTTP REST) ---

# /ping auth nélkül → mindig 200 (health-check kivétel)
curl -sf http://localhost:7778/ping   # → {"pong":true}

# Auth nélkül → 401 Unauthorized
curl -s http://localhost:7778/key/foo
# → 401, WWW-Authenticate: Basic realm="ditto"

# Helyes auth → normál válasz
curl -s -u ditto:mysecret http://localhost:7778/key/foo   # → 404 / value
curl -X PUT http://localhost:7778/key/authtest -u ditto:mysecret -d "hello"  # → 200
curl -s -u ditto:mysecret http://localhost:7778/key/authtest                 # → "hello"

# Rossz jelszó → 401
curl -s -u ditto:wrongpassword http://localhost:7778/key/foo   # → 401

# --- 7781 (ditto-mgmt UI/API) ---

# Auth nélkül → 401
curl -s http://localhost:7781/api/nodes   # → 401

# Helyes auth → JSON node lista
curl -s -u admin:mysecret http://localhost:7781/api/nodes   # → [{...},...]

# Böngészőben: http://localhost:7781/ → natív login dialógus jelenik meg

# --- ditto-mgmt → 7778 proxy (cache get/set/delete) ---
# Ha DITTO_MGMT_HTTP_AUTH_USER/PASSWORD bevan állítva:
curl -s -u admin:mysecret http://localhost:7781/api/cache/local/keys/authtest
# → {"value":"hello"}   (ditto-mgmt beadja a ditto:mysecret-et a proxy requestbe)

# --- dittoctl (hash generálás) ---
dittoctl hash-password
# Enter password: ****
# $2b$12$...
```

---

## 17. Összetett forgatókönyvek

### 16.1 TTL + Compression

```powershell
$bigval = "abcdefgh" * 1024   # 8 KB
ditto cache set local bigttl $bigval --ttl 10
ditto cache get key local bigttl
# compressed: true, ttl: ~10s

Start-Sleep 11
ditto cache get key local bigttl   # → NotFound (lejárt + tömörítve volt)
```

### 16.2 Value-size-limit + Write forwarding

```powershell
# node-2 legyen primary, node-1-re küldünk túl nagy értéket
ditto node set primary 127.0.0.1:7789 true
ditto node set value-size-limit 127.0.0.1:7789 512

$toobig = "x" * 1000
curl -X PUT http://localhost:7778/key/limitfwd -d $toobig
# node-1 forward-olja node-2-re (primary), ott elutasítja → ValueTooLarge

# Visszaállítás
ditto node set value-size-limit 127.0.0.1:7789 0
ditto node set primary 127.0.0.1:7789 false
```

### 16.3 Teljes állapot összehasonlítás

```powershell
# Minden node tulajdonságai egyszerre
ditto node describe all
# Figyeld: committed-index azonos-e, compression settings megegyeznek-e

# Cluster egészségi összefoglaló
ditto cluster list nodes
ditto cluster get status
```

---

## 18. Seeds / config tesztek

```powershell
# Seeds frissítés (minden node elérhető legyen "all" target esetén)
ditto node set seeds local "127.0.0.1:7779,127.0.0.1:7789,127.0.0.1:7799"

# Ellenőrzés
ditto node describe all   # 3 külön block jelenik meg

# Timeout csökkentés (lassabb tesztelési környezetben növeld)
ditto node set timeout local 1000   # 1000 ms

# Output format
ditto node set format local json   # ahol értelmezett, JSON outputot ad
ditto node set format local binary # visszaállítás
```

---

## 19. Hibaforgatókönyvek összefoglalója

| Forgatókönyv | Várt hiba / HTTP kód |
|---|---|
| Get nem létező kulcsra | `NotFound` / 404 |
| Write inaktív node-ra | `NodeInactive` / 503 |
| Write 1 aktív node-nál (3-ból) | `NoQuorum` / 503 |
| Value > value-size-limit | `ValueTooLarge` / 400 |
| Kulcs beírása max-keys-nél | `KeyLimitReached` |
| Write timeout (lassú replika) | `WriteTimeout` / 504 |
| TLS disabled dittoctl → TLS porthoz | `early eof` |
| `compression-threshold` csökkentése | warning / no-op (érték nem változik) |
| `node set primary node-2 true` | `cannot resolve target 'node-2'` |
| Negatív / érvénytelen port | warning / no-op |
| HTTP kérés auth nélkül (7778, ha engedélyezve) | 401 Unauthorized |
| HTTP kérés rossz jelszóval (7778) | 401 Unauthorized |
| Mgmt API kérés auth nélkül (7781, ha engedélyezve) | 401 Unauthorized |
| `/ping` auth nélkül (7778, még ha auth engedélyezve) | 200 OK |

---

## Gyors referencia

### dittoctl target stringek

| Target | Mit jelent |
|---|---|
| `local` | 127.0.0.1:7779 (node-1, a config-ban lévő cluster_port) |
| `all` | összes seed a config-ból |
| `127.0.0.1:7779` | node-1 explicit |
| `127.0.0.1:7789` | node-2 |
| `127.0.0.1:7799` | node-3 |

### Összes settable property

| Property | Értéktípus | Restart? | Példa |
|---|---|---|---|
| `status` | `true`/`active` vagy `false`/`inactive` | nem | `ditto node set status local false` |
| `primary` | `true`/`false` | nem | `ditto node set primary 127.0.0.1:7789 true` |
| `max-memory` | `<n>` vagy `<n>mb` | nem | `ditto node set max-memory local 512` |
| `default-ttl` | másodperc (0=kikapcs) | nem | `ditto node set default-ttl local 3600` |
| `value-size-limit` | byte (0=korlátlan) | nem | `ditto node set value-size-limit local 0` |
| `max-keys` | db (0=korlátlan) | nem | `ditto node set max-keys local 100000` |
| `compression-enabled` | `true`/`false` | nem | `ditto node set compression-enabled local false` |
| `compression-threshold` | byte (min 4096, csak növelhető) | nem | `ditto node set compression-threshold local 8192` |
| `client-port` | 1-65535 | **igen** | `ditto node set client-port local 7800` |
| `http-port` | 1-65535 | **igen** | `ditto node set http-port local 7900` |
| `cluster-port` | 1-65535 | **igen** | `ditto node set cluster-port local 7810` |
| `gossip-port` | 1-65535 | **igen** | `ditto node set gossip-port local 7820` |
| `seeds` | vesszővel elválasztott `host:port` lista | csak local | `ditto node set seeds local "127.0.0.1:7779,..."` |
| `timeout` | ms | csak local | `ditto node set timeout local 3000` |
| `format` | `binary`\|`json` | csak local | `ditto node set format local binary` |

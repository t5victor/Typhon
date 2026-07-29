# Thyphon — intención técnica y journey de ingeniería

> Este documento describe qué se está construyendo, por qué se eligieron sus
> límites y cómo ha evolucionado desde la idea inicial hasta el estado actual.
> Es deliberadamente más amplio que el README: funciona como mapa técnico,
> registro de decisiones y backlog vivo.

## 0. La idea en una frase

**Thyphon es un laboratorio de sistemas distribuidos con forma de mercado de
materias primas:** compañías autónomas compiten por lotes escasos, las decisiones
se convierten en hechos inmutables y las distintas vistas del sistema se
reconcilian de forma eventual.

Los minerales, los precios y los agentes son el vehículo. El objetivo real es
hacer visibles y verificables problemas que un CRUD normal no genera de forma
natural: colisiones de escritura, reintentos, duplicados, publicaciones fallidas,
proyecciones retrasadas, workflows compensatorios y reconstrucción histórica.

El nombre del producto es siempre **Thyphon**. `thyphon` es únicamente el nombre
del paquete Python.

## 1. Resultado que se quiere conseguir

La experiencia deseada tiene dos caras complementarias:

1. Una consola ASCII con `curses` donde se percibe un mercado vivo: variación
   determinista de precios, empresas con estrategias diferentes, pujas, tape de
   actividad, barras, colores, velocidad y pausa.
2. Un backend que trata esa actividad como un sistema distribuido serio: cada
   intención se valida sobre un agregado, se guarda como evento, se publica por
   outbox y actualiza modelos de lectura independientes.

La TUI local no pretende ser un dashboard de Kafka. Es una simulación
determinista y explícitamente etiquetada como tal. La topología PostgreSQL +
Kafka es el camino de ejecución distribuido.

## 2. Qué problema de negocio modela hoy

El slice implementado es una **subasta competitiva de un lote finito**:

```text
Proveedor abre un lote
        ↓
Compañías colocan ofertas estrictamente mejores
        ↓
Operador acepta la oferta líder
        ↓
WinningBidAccepted
        ↓
Process manager solicita el Settlement causal
        ↓
Proveedor de pagos confirma o rechaza
        ↓
Si el dinero llega tarde tras un rechazo: RefundRequested
        ↓
RefundCompleted o RefundFailed
```

El modelo no afirma todavía tener inventarios, envíos, reserva real de fondos,
expiradores programados ni una economía de múltiples mercados. Esos temas están
en el backlog porque necesitan reglas e invariantes propias, no columnas nuevas
en una tabla existente.

## 3. Principios que no se negocian

### Intención antes que CRUD

Los comandos son decisiones de negocio y los eventos son hechos que ya han
ocurrido. Por ejemplo:

```text
OpenAuction              → AuctionOpened
PlaceCompetitiveBid      → CompetitiveBidPlaced
AcceptWinningBid         → WinningBidAccepted
RequestSettlement        → SettlementRequested
```

No se usan títulos como `CreateAuction`, `BidUpdated`, `SetStatus` o
`DeleteSettlement`. Ocultan el motivo de la transición y hacen más difícil
razonar sobre los eventos dentro de un año. La suite incluye una prueba de AST y
de estructura que rechaza vocabulario CRUD y exige una carpeta por comando o
evento.

### Event sourcing donde importa

Los agregados `Auction`, `Company` y `Settlement` se rehidratan reproduciendo
todos los eventos de su stream. No hay snapshots: es una decisión consciente
para mantener la auditoría y el comportamiento didácticos. La contrapartida es
que streams muy largos acabarán costando más al rehidratar; se medirá antes de
considerar cambiar esta decisión.

### CQRS real, no decorativo

La decisión se toma exclusivamente con el agregado rehidratado. Las consultas
leen `auction_overview`, una proyección dedicada; nunca se usa la proyección
para validar un comando. Es normal que la vista tarde brevemente en alcanzar el
evento y la API ofrece `minimum_version` para expresar esa consecuencia.

### Cada abstracción aísla una dependencia real

No se introdujo una interfaz por cada clase. Las fronteras que sí existen son
las que cambian el comportamiento o la infraestructura: Event Store, outbox,
broker, proyector, reloj implícito de eventos, adaptador SQLite y adaptador
PostgreSQL.

## 4. Arquitectura actual

```text
                 FastAPI / comandos HTTP autenticados
                                  |
                                  v
                Command handler + agregado rehidratado
                                  |
                                  v
     PostgreSQL: event_stream + outbox + receipt (misma transacción)
                                  |
                                  v
                        outbox worker -> Kafka
                                  |
                                  v
              projection worker + settlement process manager
                         |                         |
                         v                         v
             auction_overview / receipts      SettlementRequested
```

El flujo de escritura no depende de Kafka para aceptar una intención: primero se
confirma el hecho en PostgreSQL y, en la misma transacción, se registra su
publicación pendiente. El outbox worker publica después con semántica al menos
una vez.

## 5. Tecnología por responsabilidad

| Área | Tecnología | Uso concreto | Motivo |
|---|---|---|---|
| Lenguaje | Python 3.13 | Dominio, workers, scripts y TUI | Expresivo para modelado y automatización. |
| API | FastAPI + Pydantic | Fachada de comandos y queries | Contratos HTTP y validación declarativa. |
| Persistencia | PostgreSQL 17 | Event store, receipts, outbox, read models y fallos | Transacciones, restricciones, `jsonb`, locks y operaciones reproducibles. |
| Driver y pool | psycopg 3 + psycopg-pool | Una conexión por operación | Evita compartir sesión/transacción entre threads de FastAPI. |
| Broker | Kafka | Entrega de envelopes del outbox a consumidores | Hace explícitas partición, reintento y entrega al menos una vez. |
| TUI | biblioteca estándar `curses` | Pantalla alternativa, input y color ASCII | Redibujo in-place sin crecimiento del scrollback. |
| Simulación local | SQLite estándar | Event store hermético y proyector local | Tests rápidos y TUI determinista sin servicios externos. |
| Build/test | Bazel | Targets de la suite Python | Ejecución y organización reproducibles. |
| Calidad | Ruff + Pyright strict | Estilo, errores estáticos y tipos | Evita ocultar imports o tipos opcionales frágiles. |
| Entorno | Docker Compose | PostgreSQL, Kafka, API y workers | Topología local aislada y repetible. |
| CI | GitHub Actions | Bazel, estático e integración Compose | Previene que una mejora local rompa el camino distribuido. |

Las imágenes base se fijan por digest y las acciones de CI por SHA. El contexto
de Docker excluye `.env`, caches, egg-info y artefactos de herramientas para no
filtrar secretos ni inflar builds.

## 6. Modelo de dominio y streams

Los IDs de stream están namespaced:

```text
auction:{auction_id}
company:{company_id}
settlement:{settlement_id}
```

Esto evita que dos agregados de tipos distintos compartan accidentalmente
historial. La función `stream_key()` rechaza IDs vacíos o con `:`.

### Auction

Invariantes principales:

- Un lote abre una sola vez y con cantidad/precio de reserva positivos.
- Una oferta competitiva debe superar la líder actual.
- Solo la oferta líder puede convertirse en ganadora.
- Una subasta ya asignada o expirada no vuelve a aceptar pujas.

### Company

Representa entrada al mercado y cambio explícito de apetito de riesgo. Por ahora
no valida capital antes de una puja: esa reserva de fondos forma parte de un
slice posterior.

### Settlement

La obligación económica se crea únicamente a partir de `WinningBidAccepted`.
El comando actual exige `winning_bid_event_id`; el evento histórico v1 se puede
leer con ese campo nulo mediante upcasting, pero no se puede producir un nuevo
Settlement sin causalidad.

El caso tardío está modelado como compensación:

```text
SettlementRejected
  + llegada tardía de dinero
      → LateSettlementDetected
      → RefundRequested(provider_reference)
      → RefundCompleted | RefundFailed, con la misma referencia
```

No se permite completar o fallar un reembolso con otra referencia de proveedor.

## 7. Event store: diseño y garantías

`event_stream` conserva, por evento:

- `event_id` inmutable y único;
- stream y versión del stream;
- posición global para replay ordenado;
- nombre, payload y schema version;
- correlación, causalidad, actor y tenant.

`event_stream_head` mantiene la versión actual por stream. Durante un append:

1. Se toma un advisory lock derivado de la `idempotency_key`.
2. Se vuelve a leer el receipt dentro de ese lock.
3. Se bloquea la cabecera del stream.
4. Se compara la versión esperada.
5. Se insertan eventos, envelopes de outbox, head y receipt en una transacción.

El índice único `(stream_id, stream_version)` sigue siendo la última barrera
física. El lock de idempotencia resuelve el caso que un `SELECT ... FOR UPDATE`
no podía resolver cuando todavía no existía ninguna fila de receipt.

### Idempotencia

Un receipt contiene:

```text
idempotency_key + stream_id + command_name + request_hash + actor_id + tenant_id
```

La misma clave con la misma intención e identidad devuelve la versión ya
aceptada. Si cambia stream, payload, comando, actor o tenant, se responde como
conflicto de idempotencia; no se degrada a `UniqueViolation` ni a un 500.

### Causalidad financiera persistente

`settlement_causation_claim` reserva globalmente un `winning_bid_event_id` para
un único stream de Settlement. No depende solo del ID determinista usado por el
worker: la propia persistencia impide dos Settlements para el mismo hecho.

`provider_reference_claim` hace lo mismo para referencias de confirmación de
proveedor entre Settlements.

## 8. Evolución de contratos y upgrade de datos

Los envelopes se versionan. El lector contiene un seam de upcasting puro; por
ejemplo, `SettlementRequested` v1 se adapta a v2 preservando que la causalidad
histórica era desconocida.

La migración 005 resuelve la incompatibilidad que existía con los primeros
volúmenes, cuyos streams no llevaban namespace. Clasifica los eventos históricos
y convierte coordinadamente:

- `event_stream.stream_id`;
- `event_stream_head`;
- `command_receipt.stream_id`;
- `provider_reference_claim.settlement_stream_id`;
- `transactional_outbox.partition_key` y su envelope JSON.

La migración aborta ante una colisión de stream destino. Cada migración y su
receipt se ejecutan en una transacción: no debe quedar una conversión parcial.
La verificación de integración crea datos de forma histórica y comprueba esta
conversión antes del flujo live.

## 9. Outbox, Kafka, consumidores y DLQ

### Transactional outbox

No se intenta publicar directamente después del commit. Cada evento guarda un
envelope en `transactional_outbox` dentro de la misma transacción. El worker
selecciona pendientes con `FOR UPDATE SKIP LOCKED`, publica en Kafka y marca
`published_at`.

Esto acepta que pueda ocurrir duplicación: si Kafka recibe el mensaje pero la
marca de publicación no llega a confirmarse, el worker lo reenviará. Es la
garantía correcta para este diseño: **al menos una vez** en transporte y
procesamiento idempotente en negocio/proyección.

### Proyección y process manager

El worker de proyección guarda un receipt por `(consumer_name, event_id)`. Si
recibe un duplicado no vuelve a cambiar el read model. Al observar
`WinningBidAccepted`, emite el comando interno `RequestSettlement`, con:

- `correlation_id` heredado;
- `causation_id` igual al evento ganador;
- una idempotency key derivada del evento;
- el ID causal persistente.

### Poison events y redrive

Un evento que falla se intenta tres veces; después se registra en
`projection_failure`, se publica en `thyphon.domain-events-dlq` y se avanza el
offset para no bloquear toda la partición.

El redrive no borra el historial ni marca éxito por adelantado:

```bash
python -m thyphon.workers.redrive <event-id>
```

Solo reinyecta el envelope y deja constancia de `redriven_at` y el contador. El
worker marca `resolved_at` únicamente tras ejecutar con éxito su ruta normal.
Los receipts siguen siendo la defensa final contra duplicados.

## 10. Query side y consistencia eventual

La proyección PostgreSQL `auction_overview` contiene una forma optimizada para
consultar disponibilidad/líder/estado del lote. La ruta de query puede devolver
`202` y `Retry-After` si aún no ha alcanzado `minimum_version`; no deja un
thread de API bloqueado haciendo long polling.

El rebuild no borra la tabla visible y luego la rellena evento a evento. Crea
una shadow table, reproduce la historia bajo el mismo advisory lock del
consumidor, intercambia las tablas dentro de una transacción y reconstruye los
projection receipts. Durante el rebuild se observa la vista anterior o la
nueva, no una vista vacía/parcial.

El comando operativo es intencionadamente administrativo y no está expuesto en
FastAPI:

```bash
python -m thyphon.projections.rebuild
```

## 11. API, identidad y callbacks de pago

La API diferencia quién puede expresar cada intención:

- `supplier` abre una subasta;
- `bidder` solo puja por su propia compañía;
- `operator` acepta la oferta ganadora;
- `payment-provider` informa callbacks financieros.

Las identidades del laboratorio viven en `THYPHON_API_KEYS`; cada una incluye
actor, rol y tenant. Los recibos de idempotencia quedan ligados a esa identidad.
No es un sistema de autenticación de producción: en un despliegue real habría
un proveedor de identidad, rotación y secretos gestionados.

Los callbacks financieros requieren HMAC SHA-256 de una forma canónica que
incluye `settlement_id`, intención, idempotency key, payload y timestamp. Se
usa comparación de tiempo constante y se rechazan timestamps fuera de cinco
minutos. La idempotency key firmada hace que un replay exacto sea seguro; una
integración real debe además adoptar el ID/nonce durable del proveedor.

Los IDs de path y cabeceras externas tienen límites de longitud y patrones. Un
ID con `:` ya no llega a `stream_key()` para convertirse en error 500.

## 12. TUI y simulación determinista

La TUI está construida con el buffer alternativo de `curses`, no mediante
`print()` repetidos. Por eso redibuja la pantalla completa sin convertir la
terminal en un log infinito.

Controles actuales:

| Tecla | Efecto |
|---|---|
| `Q` | Salir limpiamente de la TUI. |
| `Space` | Pausar/reanudar la simulación. |
| `+` / `-` | Aumentar/reducir la velocidad. |
| `R` | Reiniciar la simulación con la misma seed. |
| `N` | Introducir una seed nueva y reiniciar. |

La seed controla precios, selección de agentes y actividad. Es posible
reproducir una sesión, comparar cambios o convertir un fallo visual en un caso
de prueba. La simulación usa SQLite y un dispatcher local de outbox para
mostrar la misma forma de flujo, sin afirmar que sea telemetría de los workers
de Kafka.

Inicialmente se revisaba y decodificaba todo el histórico de SQLite en cada
tick. Eso hacía que una sesión larga tendiera a coste cuadrático. Ahora se usa
`global_position` y `events_after()` para proyectar solo eventos nuevos, y
`event_count()` para el panel de métricas.

El launcher macOS construye Bazel de forma silenciosa y ejecuta el binario
generado directamente; así los logs de Bazel no compiten con la pantalla de
`curses` y la ventana no se cierra automáticamente al salir.

## 13. Operación local

Compose define servicios aislados bajo el proyecto `thyphon-live`:

```text
postgres          event store y proyecciones
kafka             broker de eventos
migrate           migraciones ordenadas antes de API/workers
api               FastAPI
outbox-worker     publicación fiable hacia Kafka
projection-worker proyección y process manager
```

Los health checks representan dependencias reales: PostgreSQL debe aceptar
conexiones, Kafka debe responder a metadata y la API debe poder comprobar store
y broker mediante `/health/ready`. La comprobación de broker de la API se
cachea brevemente para que un endpoint público de readiness no cree un producer
nuevo en cada llamada.

El launcher es `scripts/launch_thyphon.zsh`; comprueba Docker Desktop, levanta
solo los servicios de Thyphon que falten y abre la consola en otra terminal. No
debe ejecutarse desde automatización sin que el operador lo haya decidido.

## 14. Estrategia de pruebas y qué demuestra cada capa

| Capa | Ejemplos | Garantía |
|---|---|---|
| Dominio | subasta, Settlement, Company | Invariantes y Given/When/Then implícito en eventos. |
| Contrato | envelope y schema version | Lectores y consumidores no pierden campos de trazabilidad. |
| Nomenclatura | AST y estructura de carpetas | No se infiltra CRUD ni se agrupan comandos/eventos. |
| TUI/simulación | render y mercado determinista | Presentación y reproducibilidad sin Docker. |
| Integración PostgreSQL | `verify_postgres_concurrency.py` | Retries idénticos y conflicto entre actor/tenant/stream. |
| Upgrade | `verify_legacy_upgrade.zsh` | Migración de streams históricos y outbox. |
| Live | `verify_live.zsh` | Auth, HMAC, Kafka, outbox, proyección, refund, rebuild y DLQ redrive. |

Bazel organiza la suite hermética con `bazel test //...`. CI instala también
los extras runtime, ejecuta Ruff y Pyright strict, y levanta Compose para la
parte de integración. Los scripts de integración existen para que el CI y el
operador puedan ejecutarlos; no sustituyen la observación de métricas bajo
carga.

### Estado de verificación honesto

La sintaxis Python modificada, los scripts Zsh y el diff se han comprobado
estáticamente. La ejecución de Bazel, Compose, Kafka, TUI y scripts live debe
hacerla el operador cuando lo decida; no se debe afirmar que esa ejecución ha
ocurrido si no hay evidencia de una corrida concreta.

## 15. Errores encontrados y cómo cambiaron el diseño

| Hallazgo | Riesgo | Corrección aplicada |
|---|---|---|
| TUI estática o redibujada con `print()` | No muestra mercado vivo y crece el scrollback | Loop `curses`, buffer alternativo, controles y simulación incremental. |
| Logs de Bazel visibles en la consola | Experiencia confusa y posible cierre de terminal | Build silencioso y ejecución directa del binario TUI. |
| Streams tempranos sin namespace | Agregados invisibles/colisión al actualizar | Migración 005 transaccional y test de upgrade. |
| Refund finalizable con cualquier referencia | Integridad de compensación débil | Referencia del `RefundRequested` se conserva y se compara. |
| `SELECT ... FOR UPDATE` sin receipt | Carrera de idempotencia cuando no hay fila | Advisory lock por clave, relectura dentro de transacción. |
| Conexión PostgreSQL compartida | Throughput bajo y transacciones mezcladas | `ConnectionPool`, checkout por operación y cierre explícito. |
| Rebuild visible a medio camino | Queries con 404 o estado incompleto | Shadow table e intercambio transaccional. |
| Poison event bloqueando consumo | Una partición deja de avanzar | Reintento acotado, DLQ, redrive y receipt idempotente. |
| HMAC sin vida útil | Replays temporales de callback | Timestamp firmado y ventana de cinco minutos. |
| `.env` en build context | Exposición en builder/cache remoto | Exclusiones explícitas en `.dockerignore`. |
| Simulación releyendo historial completo | Coste cuadrático en sesiones largas | Cursor por posición global y contador SQL. |

## 16. Limitaciones conocidas

Estas no son deudas ocultas; delimitan conscientemente el alcance actual:

- No hay reserva de capital ni verificación de Company antes de pujar.
- No hay deadline real ni scheduler para `ExpireAuction`.
- No hay envío, recepción, inventario, unidades o moneda múltiple.
- No hay reasignación de un lote tras fallo de Settlement.
- Los agentes son estrategias deterministas codificadas; no hay plugins ni IA.
- Kafka se usa sin Schema Registry ni contratos externos versionados.
- No hay métricas Prometheus, trazas OpenTelemetry ni dashboard Grafana.
- No hay multi-tenancy real más allá de propagación/aislamiento de identidad.
- No hay nonce durable de proveedor: el callback firmado es seguro por
  idempotencia y ventana temporal, pero un proveedor real debe aportar su ID de
  evento como clave de replay de larga duración.
- No se toman snapshots por decisión de diseño; hay que medir antes de cambiar
  este límite para streams de gran tamaño.

## 17. TODO priorizado

### P0 — mantener la confianza en la base actual

- [ ] Ejecutar y guardar evidencia de `bazel test //...` tras cada cambio.
- [ ] Ejecutar la integración Compose y los tres scripts de verificación en CI.
- [ ] Añadir una prueba HTTP hermética para auth, límites de entrada y HMAC,
      cuando la dependencia FastAPI esté declarada también para ese target Bazel.
- [ ] Validar el upgrade sobre una copia anonimizada de un volumen real antiguo
      antes de usarlo fuera del laboratorio.

### P1 — completar el modelo de asignación

- [ ] Añadir `Company` como precondición de `PlaceCompetitiveBid` y reservar
      fondos antes de aceptar una oferta.
- [ ] Modelar una ventana de subasta con deadline y un worker/scheduler que
      emita `AuctionExpired` de manera idempotente.
- [ ] Diseñar la liberación/reasignación del lote tras rechazo o timeout de
      Settlement, con eventos compensatorios explícitos.
- [ ] Introducir value objects de moneda, unidad de medida y semántica clara de
      precio unitario frente a total.
- [ ] Añadir un ID de callback/nonce durable del proveedor y su claim
      persistente para la integración de pagos real.

### P2 — observabilidad y operación

- [ ] Exponer métricas de comandos, eventos, conflictos, retraso de proyección,
      outbox y DLQ con Prometheus.
- [ ] Instrumentar API, PostgreSQL y Kafka con OpenTelemetry y correlation IDs.
- [ ] Añadir Grafana con paneles de operaciones equivalentes a los de la TUI.
- [ ] Crear herramienta de inspección/listado de DLQ y un workflow de redrive
      por lote con auditoría de operador.
- [ ] Definir retención/archivado de `command_receipt`, outbox publicado y
      fallos resueltos sin perder evidencias necesarias.

### P3 — convertir el laboratorio en simulador económico más rico

- [ ] Más lotes simultáneos, proveedores y mercados por recurso.
- [ ] Eventos macroeconómicos deterministas: embargo, sequía, descubrimiento,
      cambio de impuesto y alteración de demanda.
- [ ] Estrategias cargables por plugin, con sandbox y límites de CPU/tiempo.
- [ ] Inventario, contratos de envío, clima y riesgo de proveedor.
- [ ] Benchmark reproducible: concurrencia por recurso, duplicados, caída de
      consumidor, redrive, rebuild y medición de throughput/latencia.

## 18. Cómo leer el repositorio

Empieza por el agregado y sigue una línea vertical:

```text
domain command
  → aggregate method
  → domain event
  → application handler
  → event store / outbox
  → Kafka worker
  → projection or process manager
  → query/TUI/operación
```

Puntos de entrada útiles:

- `src/thyphon/auction/domain/` — decisiones y hechos de subasta.
- `src/thyphon/settlement/domain/` — workflow financiero y compensación.
- `src/thyphon/infrastructure/postgres_event_store.py` — append transaccional,
  concurrencia e idempotencia.
- `src/thyphon/workers/main.py` — outbox, consumidor y process manager.
- `src/thyphon/projections/postgres_auction_overview.py` — query side y rebuild.
- `src/thyphon/tui/live.py` — consola `curses` y controles.
- `scripts/` — launcher, benchmark y verificaciones de integración.

## 19. Criterio de éxito

Thyphon tendrá valor como pieza de portfolio si permite explicar, demostrar y
medir con honestidad estas preguntas:

- ¿Por qué este dominio necesita CQRS/Event Sourcing y cuándo no lo necesitaría?
- ¿Qué protege la invariante si dos actores pujan a la vez?
- ¿Qué ocurre si Kafka acepta un mensaje pero PostgreSQL no marca el outbox?
- ¿Cómo se comporta un retry idéntico, uno con otra identidad y un duplicado de
  consumidor?
- ¿Cómo se recupera una proyección eliminada o un poison event?
- ¿Cómo evoluciona un evento que ya está en producción?
- ¿Qué parte es consistente inmediatamente y qué parte es eventualmente
  consistente?

Mientras cada respuesta esté respaldada por código, una prueba y un trade-off
explícito, el proyecto seguirá siendo un laboratorio técnico útil y no una
colección de palabras de moda.

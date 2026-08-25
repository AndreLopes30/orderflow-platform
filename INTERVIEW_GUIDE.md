# OrderFlow Platform — Interview Guide

Este guia descreve o projeto que está no repositório. A melhor defesa em entrevista é explicar as garantias, as janelas de falha e os trade-offs sem chamar o sistema de “exactly once”.

## 1. Narrativa de 90 segundos

> O OrderFlow é formado por dois microsserviços Spring Boot 4.1 em Java 21. O order-service recebe e valida pedidos, persiste PostgreSQL e grava um OrderCreatedEvent em uma Transactional Outbox na mesma transação. Um scheduler concorrente publica a Outbox no Kafka com orderId como key. O notification-service consome com semântica at-least-once, usa eventId como chave primária numa tabela processed_events e grava a notificação na mesma transação, então redeliveries não repetem o efeito. Falhas recuperáveis passam por dois retry topics com backoff exponencial e falhas esgotadas são gravadas numa DLT e numa tabela auditável. Redis é usado apenas como cache de GET com TTL de dez minutos e invalidação após mudança de status. O ambiente Compose inclui Prometheus, Grafana, OpenTelemetry Collector e Tempo; CI executa Maven verify, Testcontainers e build das imagens.

Pratique até conseguir falar isso sem decorar palavras.

## 2. O que estudar primeiro

1. fluxo de um `POST /orders` até `notification_records`;
2. por que Order + Outbox estão na mesma transação;
3. onde ainda pode ocorrer duplicata;
4. como `processed_events` fecha essa janela;
5. partitions, key, consumer group e offsets;
6. diferença entre retry topic e DLT;
7. cache-aside, TTL e invalidação;
8. métricas, traces e logs do projeto;
9. Docker, probes e scaling Kubernetes;
10. trade-offs e melhorias.

## 3. Java e Spring Boot

### Por que Java 21?

É uma versão LTS, com runtime moderno e suporte do Spring Boot 4.1. O projeto usa records para DTOs/eventos, switch expressions no state machine e APIs modernas de data/tempo. Virtual threads não foram adicionadas porque o ganho não era necessário para provar o fluxo e a stack de drivers já atende o volume de demo.

### Por que records para DTOs e eventos?

São imutáveis, expressam value objects pequenos e reduzem boilerplate. Entidades JPA continuam classes mutáveis porque o ORM exige construtor e lifecycle próprios.

### O que `@Transactional` realmente faz?

Spring cria um proxy. Uma chamada externa ao bean abre a transação antes do método e decide commit/rollback ao retornar. Uma chamada de um método para outro no mesmo objeto não atravessa o proxy. Por isso `OutboxPublisher` chama `OutboxStateService`, e o listener chama `NotificationProcessor`/`DeadLetterRecorder`.

### Por que DTOs não são entidades?

Evita expor lazy loading, campos internos como `version`, mutabilidade do ORM e mudanças de schema diretamente na API. `OrderResponse.from` faz o mapeamento explícito.

### O que faz o tratamento global de exceções?

`ApiExceptionHandler` transforma not found, transição inválida, body malformado e Bean Validation em RFC Problem Details consistente, com `code`, timestamp e trace ID quando disponível.

## 4. PostgreSQL e transações

### Por que PostgreSQL?

O problema tem estado relacional, constraints, transações e concorrência. PostgreSQL fornece `JSONB` para a Outbox, `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, índices parciais, UUID e timestamps com timezone.

### Por que `BigDecimal` e `NUMERIC(19,2)`?

`double` representa números binários aproximados e pode introduzir erro monetário. `BigDecimal` e `NUMERIC` mantêm precisão decimal. Bean Validation e `CHECK` defendem duas fronteiras.

### Para que serve `@Version`?

Hibernate usa optimistic locking. Dois updates baseados na mesma versão não sobrescrevem silenciosamente; o segundo detecta conflito. Isso é apropriado quando colisões são raras.

### Por que Flyway e `ddl-auto=validate`?

Flyway versiona mudanças reproduzíveis. Hibernate valida o mapping, mas não modifica produção implicitamente. A migration vira parte revisável do deploy.

### O que são índices parciais?

`idx_outbox_pending` contém somente linhas com `published_at IS NULL`. O publisher busca exatamente esse conjunto, então o índice não cresce com todo o histórico publicado da mesma forma que um índice completo.

### Qual o isolation level?

O projeto usa o padrão PostgreSQL `READ COMMITTED`. O lock explícito de linha e `SKIP LOCKED` resolvem o claim da Outbox. A chave primária e `ON CONFLICT` resolvem a corrida de idempotência sem exigir serializable.

## 5. Kafka

### Por que Kafka?

Porque o projeto precisa de log durável, replay, consumer groups, particionamento e desacoplamento temporal. O `POST` não espera o notification-service. Kafka é mais do que uma fila: mantém registros conforme retenção e cada group possui offsets próprios.

### O que é um topic?

Um log nomeado e particionado. Aqui o principal é `order.created`, com três partitions. Retry e DLT são outros topics.

### O que é uma partition?

Um log ordenado. A partition é a unidade de ordering e paralelismo dentro de um group. Um consumer ativo por partition recebe registros por vez; três partitions permitem até três assignments concorrentes.

### Por que `orderId` como key?

Kafka faz hash da key e escolhe a partition. A mesma key normalmente permanece na mesma partition do mesmo topic, garantindo ordem por pedido. Usar `eventId` distribuiria eventos do mesmo pedido e perderia essa garantia.

### O que é consumer group?

Consumers com o mesmo group dividem as partitions. `notification-service-v1` processa cada registro uma vez por group em condições normais. Outro group poderia construir outra projeção sem competir com ele.

### O que é offset?

É a posição de um group numa partition. Commitar offset diz “este group avançou até aqui”; não prova que um e-mail externo ocorreu exactly-once.

### O que causa rebalance?

Entrada/saída de consumers, mudança de partitions, timeouts ou alteração de subscription. Partitions são redistribuídas. Um registro processado sem offset commitado pode ser entregue à nova dona.

### O producer é idempotente?

Sim, `enable.idempotence=true` e `acks=all` evitam duplicatas causadas por retry interno do producer dentro de uma sessão e preservam ordering compatível. Isso não resolve um novo send da aplicação após crash entre ACK e `published_at`.

### O que `acks=all` significa em single-node?

O líder e todas as réplicas in-sync confirmam. No Compose há apenas uma réplica, então a garantia local não demonstra tolerância a falha de broker. Em produção, replication factor e `min.insync.replicas` precisam ser maiores.

### Como o JSON é serializado?

O `order-service` usa o Jackson 3 `ObjectMapper` para transformar `OrderCreatedEvent` em String antes de gravar a Outbox. Kafka usa `StringSerializer`. O consumer usa `StringDeserializer` e Jackson para reconstruir seu próprio record. Assim o JSON persistido é exatamente o publicado.

### Para que serve `eventVersion`?

Compatibilidade de contrato. O consumer aceita v1 e classifica versões desconhecidas como irrecuperáveis. Em evolução breaking, primeiro publique um consumer que entenda v1/v2, depois habilite o producer v2.

## 6. At-least-once e idempotência

### O que significa at-least-once?

O sistema prioriza não perder: um evento será entregue uma ou mais vezes. Duplicatas são possíveis. Isso é diferente de at-most-once, que pode perder, e de exactly-once, que exige definir exatamente qual fronteira está coberta.

### Cite a janela de duplicação principal.

Kafka confirma o publish; antes de marcar `outbox_events.published_at`, o processo morre. A linha é republicada depois. O mesmo `eventId` chega duas vezes.

### Como a idempotência funciona?

`INSERT processed_events ... ON CONFLICT DO NOTHING` retorna 1 para o primeiro processamento e 0 para duplicata. A mesma transação grava `notification_records`. A PK toma a decisão atomicamente mesmo com consumers concorrentes.

### Por que não usar `existsById` antes de inserir?

É check-then-act: duas threads podem ler “não existe” e executar o efeito. O insert protegido por constraint deixa o banco arbitrar atomicamente.

### O que acontece se a falha ocorre depois do claim?

O claim e o efeito estão na mesma transação. A exceção faz rollback, então a linha `processed_events` não fica presa e o retry pode tentar de novo.

### Isso é exactly-once?

O efeito PostgreSQL fica efetivamente uma vez por `eventId`. O sistema fim a fim continua at-least-once: Kafka pode ter duplicatas, e uma futura API de e-mail externa não participaria da transação. Dizer “efeito DB idempotente” é mais correto.

### Por quanto tempo guardar `processed_events`?

Pelo menos enquanto um evento puder reaparecer: retenção/replay Kafka, retry/DLT e política operacional. Apagar cedo reabre duplicatas. Em grande volume, particione ou arquive por `processed_at` com uma política explícita.

## 7. Transactional Outbox

### Qual problema ela resolve?

O dual write: pedido commitado sem evento quando Kafka falha, ou evento publicado para um pedido que depois sofre rollback.

### Como resolve?

Grava estado e intenção de evento no mesmo PostgreSQL commit. Um processo separado publica a intenção posteriormente.

### Por que não publicar Kafka e banco na mesma `@Transactional`?

`@Transactional` JPA controla o banco. Kafka é outro recurso. Sem uma transação coordenada, não existe atomicidade conjunta só porque as linhas de código estão no mesmo método.

### Como múltiplas réplicas publicam sem duplicar normalmente?

`FOR UPDATE SKIP LOCKED` dá locks de linha durante o claim; `locked_at` mantém ownership lógico após commit. Send timeout menor que lock timeout reduz overlap. Duplicata por crash ainda é possível e esperada.

### Por que marcar published depois do ACK?

Marcar antes poderia perder o evento se o send falhar. Marcar depois pode duplicar em crash, e duplicata é tratável por idempotência.

### Por que polling, não Debezium?

Polling mantém a demo pequena, testável e sem outra plataforma. O índice parcial e batch tornam adequado ao volume alvo. CDC é uma evolução válida quando throughput/latência ou carga de polling justificarem a operação adicional.

### Como funciona o backoff da Outbox?

`min(1s × 2^attempts, 60s)`. Não descarta eventos. Em produção, alerte por idade e número de tentativas.

## 8. Retry e DLT

### Qual a diferença entre retry e DLT?

Retry trata falha possivelmente transitória e agenda nova tentativa. DLT recebe evento esgotado ou irrecuperável para investigação/reprocessamento controlado.

### Por que retry não bloqueante?

O registro sai do topic principal, então outros pedidos continuam. Blocking retry pausaria a partition durante o backoff.

### Qual o trade-off?

Ordering. Um evento posterior no principal pode ser processado enquanto o anterior espera em retry. O projeto aceita porque só há criação; uma saga multi-evento precisaria avaliar retry bloqueante, chave de versão ou state machine que rejeite fora de ordem.

### Quantas tentativas existem?

Três totais: original, `retry-0` após 1 s e `retry-1` após 2 s. Depois, DLT.

### Que erros não são retentados?

JSON inválido, versão não suportada, campos inválidos ou Kafka key diferente de `orderId`. Tempo não corrige esses dados.

### Como reproduzir DLT?

Criar pedido com `customerId` iniciado por `fail-dlt-`. O processor lança `SimulatedNotificationException` depois do claim, a transação reverte em cada tentativa e a DLT é registrada.

### Por que persistir DLT em tabela?

Facilita auditoria, busca e dashboard operacional. O topic continua sendo fonte de replay; a tabela não o substitui.

## 9. Redis

### Por que Redis está aqui?

Consultas repetidas de pedido são read-heavy e o dado muda pouco. Redis reduz latência e carga PostgreSQL. Não foi usado como decoração nem como fonte de verdade.

### Qual padrão de cache?

Cache-aside através de `@Cacheable`: procura Redis, em miss busca banco e escreve cache.

### TTL, hit e miss?

TTL 10 minutos. O primeiro GET é miss; seguintes são hits. Null/404 não entram no cache.

### Como invalida?

`PATCH /status` usa `@CacheEvict`. O `RedisCacheManager` é transaction-aware, então a eviction acontece após commit bem-sucedido.

### Por que JSON em vez de Java serialization?

É legível, interoperável e evita riscos conhecidos de desserialização nativa Java. O serializer é tipado para `OrderResponse`.

### O que é cache stampede?

Muitos misses simultâneos para a mesma key podem atingir o banco juntos. O projeto não implementa single-flight porque o volume de demo não justifica; seria um próximo passo se métricas mostrarem o problema.

## 10. Docker e Docker Compose

### O que é multi-stage build?

Maven e source ficam no estágio de build. A imagem final contém JRE e JAR, reduzindo tamanho e superfície de ataque.

### Por que usuário não-root?

Limita impacto de uma exploração no container. Não substitui seccomp, filesystem read-only e políticas do cluster, mas é baseline.

### O que healthcheck resolve?

Detecta disponibilidade real, não apenas processo existente. Compose usa `service_healthy` para dependências. Probes Java acessam os grupos de health do Actuator.

### Por que Kafka tem listener interno e externo?

Clients em containers resolvem `kafka:19092`; clients no host usam `localhost:9092`. Kafka anuncia o endereço que o client usará após bootstrap, então um único hostname não serve aos dois contextos.

## 11. Kubernetes

### Deployment vs StatefulSet?

Serviços Java, Kafka local e Redis demo usam Deployment; PostgreSQL usa StatefulSet com identidade/volume persistente. Kafka de produção não deveria ser gerenciado por esse manifesto simples — use MSK ou operador maduro.

### Readiness vs liveness?

Readiness remove o pod do Service quando não pode receber tráfego. Liveness reinicia processo travado. Reiniciar por dependência temporária pode piorar incidentes, por isso liveness usa o grupo específico do estado da aplicação, e readiness cobre dependências.

### Por que duas réplicas dos serviços?

Demonstra stateless scaling e o comportamento concorrente: Outbox faz claim seguro; Kafka reparte partitions; idempotência protege redelivery.

### Requests e limits?

Requests ajudam scheduling; limits contêm consumo. Heap Java usa `MaxRAMPercentage`, respeitando memória do container. Valores são baseline, não tuning final.

### Onde ficam secrets?

`secret.example.yml` mostra as keys, mas não é aplicado pelo Kustomization. O operador cria `orderflow-secrets` fora do Git. Produção usaria External Secrets/Secrets Manager.

## 12. Observabilidade

### Métricas, logs e traces: diferença?

- métricas mostram tendências agregadas e alertas;
- logs explicam eventos discretos com contexto;
- traces mostram causalidade/latência de uma execução distribuída.

### O que Actuator expõe?

Health/liveness/readiness, métricas e endpoint Prometheus. Também produz métricas JVM, HTTP, Hikari e Kafka via Micrometer.

### Como OpenTelemetry flui?

Spring/Micrometer cria spans, exporta OTLP/HTTP para Collector, que aplica memory limiter/batch e envia OTLP/gRPC ao Tempo. Grafana consulta Tempo.

### A trace vai do POST até a notificação?

Não completamente. A Outbox é assíncrona e não persiste `traceparent`; o span HTTP termina. O publisher inicia uma trace Kafka e o consumer continua essa trace pelos headers. `eventId` permite correlação entre as duas partes.

### Por que essa honestidade importa?

Porque observabilidade deve representar causalidade real. Propagar um contexto por horas também pode produzir uma trace longa e enganosa; span links ou correlation ID podem ser melhores.

### Quais métricas alertar?

HTTP 5xx/p95, lag, Outbox failures/idade, DLT, Hikari saturation, heap e readiness. O dashboard já mostra a maioria; idade da Outbox é melhoria pendente.

### O que são logs estruturados?

JSON com campos, não texto livre. `eventId`, `orderId`, attempt, result, traceId e spanId podem ser filtrados sem regex frágil.

## 13. Testes

### Pirâmide usada

- unitários rápidos para regras e branches de falha;
- MockMvc standalone para contrato HTTP/validação;
- Testcontainers para PostgreSQL/Kafka/Redis reais e fluxo assíncrono.

### Por que Testcontainers?

H2 não reproduz `JSONB`, `ON CONFLICT`, índice parcial nem `SKIP LOCKED`. Embedded broker também não valida a imagem Kafka real. Containers descartáveis dão fidelidade maior.

### O que o IT do order-service prova?

Migration, POST/GET/PATCH real, persistência, publicação Kafka, key/payload, marca Outbox, cache Redis e eviction.

### O que o IT do notification-service prova?

Duas mensagens iguais geram uma notificação; falha controlada percorre retries/DLT e rollback; depois um evento saudável ainda é consumido.

### O que aconteceu neste ambiente?

`mvn verify` passou com 25 unitários. Os três cenários Testcontainers foram compilados e ignorados porque não existe daemon Docker local. A GitHub Action foi configurada para executá-los em runner com Docker. Não diga que integração foi executada localmente.

## 14. CI/CD

### O pipeline faz o quê?

O job `test` configura Java 21 com cache Maven, roda `mvn verify` e falha se os relatórios Failsafe não comprovarem os três cenários Testcontainers com zero skips. O `docker-build` constrói as duas imagens via Buildx. Depois, o `e2e` sobe toda a stack com Compose, executa `smoke-test.sh`, preserva logs em falha e sempre remove containers, volumes e órfãos. Não faz push/deploy.

### Por que o job Docker depende dos testes?

Evita gastar build de imagem quando código/testes já falharam. Em times grandes, jobs poderiam rodar em paralelo para feedback mais rápido; aqui simplicidade vence.

### Como a CI evita um falso positivo quando Docker está indisponível?

Localmente, `disabledWithoutDocker=true` permite que o restante do build seja validado sem daemon. No GitHub, uma etapa lê os XMLs do Failsafe, exige dois reports, três cenários totais e `skipped=0`. Assim, o job falha se Testcontainers não usar PostgreSQL, Kafka e Redis reais.

### O que o smoke E2E comprova?

Readiness, migrations Flyway, POST/GET/PATCH, persistência, Order + Outbox, ACK e registro Kafka, consumo assíncrono, cache/TTL/eviction Redis, duplicata com o mesmo `eventId`, retries e DLT, recuperação do consumer e disponibilidade de Prometheus, Grafana, Collector e Tempo. A existência da pipeline não equivale a dizer que ela já ficou verde; isso só poderá ser afirmado após execução no runner.

### O que faltaria para CD?

Registry ECR/GHCR, tag imutável, SBOM/assinatura/scan, promoção por ambiente, migration strategy, rollout, smoke test e rollback. Credenciais via OIDC, nunca long-lived secrets.

## 15. AWS

### Mapeamento principal

- apps: ECS/Fargate ou EKS;
- PostgreSQL: RDS Multi-AZ;
- Kafka: MSK;
- Redis: ElastiCache;
- images: ECR;
- secrets: Secrets Manager;
- telemetry: ADOT, CloudWatch, Managed Prometheus/Grafana ou backend OTLP.

### ECS ou EKS?

ECS/Fargate reduz operação para poucos serviços. EKS é adequado quando a organização já padroniza Kubernetes, precisa de operators/policies/ecossistema e aceita seu custo operacional. O código é portável para ambos.

### Como evitar custo neste projeto?

Não há IaC aplicado nem pipeline de deploy. A seção AWS é desenho, não provisionamento.

## 16. Perguntas difíceis e respostas curtas

### “Por que não exactly-once?”

Kafka EOS cobre fronteiras Kafka específicas. O sistema também grava PostgreSQL e futuramente chamaria provedor externo. A garantia real é at-least-once com efeito DB idempotente por `eventId`.

### “A Outbox elimina duplicatas?”

Não. Elimina perda/inconsistência do dual write, mas pode duplicar no crash após ACK. O consumidor resolve.

### “E se duas réplicas processarem o mesmo eventId?”

Uma ganha o insert da PK; a outra recebe zero em `ON CONFLICT DO NOTHING` e não executa efeito.

### “E se Redis perder tudo?”

É cache. GETs repopulam a partir do PostgreSQL. Pedidos não são perdidos.

### “E se processed_events for apagada?”

Um replay antigo pode repetir efeitos. Retenção dessa tabela deve cobrir todo o horizonte de replay/redelivery.

### “E se a DLT handler falhar?”

`FAIL_ON_ERROR` evita ciclo de republicação na própria DLT. Sem retorno bem-sucedido, o offset DLT não avança e Kafka pode redeliver; o insert é idempotente.

### “Como reprocessar DLT?”

Após corrigir a causa, uma ferramenta operacional deve validar o payload e republicá-lo no principal ou num topic de replay com auditoria. Não automatize replay infinito.

### “Como aumenta throughput?”

Meça primeiro. Aumente partitions e réplicas do consumer, ajuste pools/batch Outbox, e depois avalie CDC. Lembre que mais partitions mudam ordering e custo.

### “O que você mudaria para produção?”

HA gerenciada, TLS/SASL/OIDC, alertas, retention/cleanup, provider real com outgoing Outbox, secrets externos, network policies, scans/SBOM, contract tests e testes de caos.

### “Por que dois bancos?”

Ownership por serviço e deploy independente. Compartilhar instância física pode ser aceitável inicialmente, mas schemas/credenciais continuam isolados.

### “Por que não microserviço de cache?”

Cache é detalhe do read path do order-service. Outro serviço adicionaria rede e ownership sem benefício.

## 17. Demonstração de 5 minutos

1. Mostre o Mermaid do README.
2. Faça um POST normal e capture `orderId`.
3. Faça dois GETs e mostre a key Redis.
4. Consulte a notificação e o group lag.
5. Mostre `orders` + `outbox_events.published_at`.
6. Faça POST `fail-dlt-demo`.
7. Mostre retry topics, DLT e `dead_letter_events`.
8. Crie um pedido normal depois, provando saúde.
9. Abra Grafana/Tempo.
10. Encerre explicando a janela ACK → `published_at` e a idempotência.

## 18. Checklist antes da entrevista

- [ ] Consigo desenhar o fluxo sem consultar o README.
- [ ] Sei dizer exatamente onde começam/terminam as transações.
- [ ] Sei explicar at-least-once com um crash concreto.
- [ ] Sei explicar por que PK é melhor que `exists`.
- [ ] Sei diferenciar key, partition, group e offset.
- [ ] Sei citar o trade-off de retry topics.
- [ ] Sei explicar TTL/hit/miss/eviction Redis.
- [ ] Sei explicar readiness vs liveness.
- [ ] Não afirmo que Docker/Testcontainers rodaram neste workspace.
- [ ] Sei propor provider externo sem quebrar idempotência.
- [ ] Sei mapear o sistema para AWS sem fingir deploy existente.

## 19. Leitura dirigida no código

Ordem sugerida:

1. `order-service/.../OrderCommandService.java`;
2. migration `V1__create_orders_and_outbox.sql`;
3. `OutboxStateService.java` e `OutboxPublisher.java`;
4. `OrderCreatedEventListener.java`;
5. `NotificationProcessor.java`;
6. migration `V1__create_notification_tables.sql`;
7. `application.yml` de ambos;
8. testes `OrderFlowIntegrationIT` e `NotificationKafkaIntegrationIT`;
9. `docker-compose.yml`;
10. `k8s/applications.yml` e workflow CI.

Ao estudar cada arquivo, responda: qual falha ele evita, qual garantia oferece e qual garantia ele não oferece?

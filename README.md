# kafka-message-utils

Kafka producer / consumer 사용할 때 반복되는 코드를 줄여주는 공통 라이브러리

---

## 목차

- [요구사항](#요구사항)
- [빌드](#빌드)
- [의존성 추가](#의존성-추가)
- [사용법](#사용법)
  - [Producer](#producer)
  - [Consumer](#consumer)
  - [JSON 직렬화](#json-직렬화)
- [유틸리티](#유틸리티)
  - [KafkaHeaderUtils](#kafkaheaderutils)
  - [KafkaTopicUtils](#kafkatopicutils)
  - [CorrelationIdUtils](#correlationidutils)
  - [KafkaMetricsUtils](#kafkametricsutils)
  - [MessageTimestampUtils](#messagetimestamputils)
  - [KafkaHealthChecker](#kafkahealthchecker)
- [배포](#배포)

---

## 요구사항

- Java 17+
- Maven 3.8+

---

## 빌드

```bash
# 컴파일 + 테스트
mvn clean verify

# 테스트 스킵
mvn clean package -DskipTests

# 로컬 Maven 저장소에 설치 (~/.m2)
mvn clean install
```

---

## 의존성 추가

### 로컬 설치 후 사용 (`mvn install` 이후)

**Maven**

```xml
<dependency>
    <groupId>io.github.indoku</groupId>
    <artifactId>kafka-message-utils</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle**

```groovy
implementation 'io.github.indoku:kafka-message-utils:1.0.0'
```

---

## 사용법

### Producer

#### 기본 설정

```java
KafkaProducerConfig config = KafkaProducerConfig.builder()
    .bootstrapServers("localhost:9092")
    .acks("all")
    .retries(3)
    .build();
```

#### Fire-and-forget (응답 대기 없음)

```java
try (KafkaMessageProducer<String, String> producer = new KafkaMessageProducer<>(config)) {
    producer.send("my-topic", "hello");
    producer.send("my-topic", "my-key", "hello");
}
```

#### Sync (브로커 ACK 대기)

```java
SendResult result = producer.sendSync("my-topic", "hello");
System.out.println("partition=" + result.getPartition() + ", offset=" + result.getOffset());
```

#### Async (CompletableFuture)

```java
producer.sendAsync("my-topic", "hello")
        .thenAccept(result -> System.out.println("Sent: " + result))
        .exceptionally(ex -> { System.err.println("Failed: " + ex.getMessage()); return null; });
```

---

### Consumer

#### 기본 설정

```java
KafkaConsumerConfig config = KafkaConsumerConfig.builder()
    .bootstrapServers("localhost:9092")
    .groupId("my-group")
    .autoOffsetReset("earliest")
    .build();
```

#### 백그라운드 폴링 (논블로킹)

```java
KafkaMessageConsumer<String, String> consumer = new KafkaMessageConsumer<>(config);

consumer.subscribe(record -> {
    System.out.println("topic=" + record.topic() + ", value=" + record.value());
}, "my-topic");

// 다른 작업 수행 가능 ...

// 종료
consumer.close();
```

#### 블로킹 폴링 (메인 스레드에서 직접 실행)

```java
KafkaMessageConsumer<String, String> consumer = new KafkaMessageConsumer<>(config);

// 이 메서드는 close() 호출 전까지 블로킹됨
consumer.subscribeAndPoll(record -> {
    System.out.println(record.value());
}, "my-topic");
```

#### 핸들러에서 예외 발생 시

핸들러 내부에서 예외가 발생해도 **폴링 루프는 중단되지 않습니다.** 에러 로그만 출력하고 다음 레코드를 계속 처리합니다.

---

### JSON 직렬화

#### Producer에 JsonSerializer 적용

```java
KafkaProducerConfig config = KafkaProducerConfig.builder()
    .bootstrapServers("localhost:9092")
    .valueSerializer(JsonSerializer.class)
    .build();

try (KafkaMessageProducer<String, OrderEvent> producer = new KafkaMessageProducer<>(config)) {
    producer.send("orders", new OrderEvent("123", "CREATED"));
}
```

#### Consumer에 JsonDeserializer 적용

```java
KafkaConsumerConfig config = KafkaConsumerConfig.builder()
    .bootstrapServers("localhost:9092")
    .groupId("order-group")
    .valueDeserializer(JsonDeserializer.class)
    .property(JsonDeserializer.TARGET_TYPE_CONFIG, OrderEvent.class.getName())
    .build();

KafkaMessageConsumer<String, OrderEvent> consumer = new KafkaMessageConsumer<>(config);
consumer.subscribe(record -> {
    OrderEvent event = record.value();
    System.out.println("orderId=" + event.orderId());
}, "orders");
```

---

## 유틸리티

### KafkaHeaderUtils

Kafka 메시지 헤더 읽기/쓰기를 간단하게 처리합니다.

```java
// 헤더 쓰기
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "value");
KafkaHeaderUtils.setString(record.headers(), "trace-id", "abc-123");

// 헤더 읽기
String traceId = KafkaHeaderUtils.getString(consumerRecord, "trace-id").orElse("unknown");

// 헤더 존재 여부
boolean exists = KafkaHeaderUtils.contains(consumerRecord, "trace-id");
```

---

### KafkaTopicUtils

AdminClient 기반 토픽 관리 유틸리티입니다.

```java
try (KafkaTopicUtils topicUtils = new KafkaTopicUtils("localhost:9092")) {
    // 토픽 존재 여부
    boolean exists = topicUtils.exists("my-topic");

    // 파티션 수 조회
    int partitions = topicUtils.partitionCount("my-topic");

    // 토픽 목록
    Set<String> topics = topicUtils.listTopics();

    // 토픽 생성 (없을 때만)
    topicUtils.createIfAbsent("new-topic", 3, (short) 1);

    // 토픽 삭제
    topicUtils.delete("old-topic");
}
```

---

### CorrelationIdUtils

메시지 추적을 위한 correlation-id 헤더 자동 생성/추출 유틸리티입니다.

```java
// Producer: correlation-id 자동 생성 후 헤더에 주입
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "value");
String correlationId = CorrelationIdUtils.inject(record);

// Producer: 직접 지정한 ID 주입
CorrelationIdUtils.inject(record, "custom-id-001");

// Consumer: 헤더에서 추출
String cid = CorrelationIdUtils.extract(consumerRecord).orElse("unknown");

// Consumer: 없으면 새로 생성해서 반환
String cid = CorrelationIdUtils.extractOrGenerate(consumerRecord);
```

---

### KafkaMetricsUtils

Consumer lag, 오프셋, 컨슈머 그룹 조회 유틸리티입니다.

```java
try (KafkaMetricsUtils metrics = new KafkaMetricsUtils("localhost:9092")) {
    // 그룹 전체 lag 합계
    long totalLag = metrics.totalLag("my-group");

    // 특정 토픽 lag
    long topicLag = metrics.totalLag("my-group", "orders");

    // 파티션별 lag
    Map<TopicPartition, Long> lagMap = metrics.lagPerPartition("my-group");

    // 커밋된 오프셋 조회
    Map<TopicPartition, Long> offsets = metrics.committedOffsets("my-group");

    // 컨슈머 그룹 목록
    Set<String> groups = metrics.listConsumerGroups();
}
```

---

### MessageTimestampUtils

Kafka 레코드 타임스탬프를 Java 시간 타입으로 변환합니다.

```java
// ConsumerRecord → Instant
Instant instant = MessageTimestampUtils.toInstant(record);

// ConsumerRecord → LocalDateTime (시스템 타임존)
LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(record);

// ConsumerRecord → LocalDateTime (UTC)
LocalDateTime utc = MessageTimestampUtils.toLocalDateTime(record, ZoneId.of("UTC"));

// epoch millis → LocalDateTime
LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(1705321800000L, ZoneOffset.UTC);
```

---

### KafkaHealthChecker

브로커 연결 상태를 확인하는 헬스 체커입니다.

```java
try (KafkaHealthChecker checker = new KafkaHealthChecker("localhost:9092")) {
    // 연결 상태 확인
    boolean healthy = checker.isHealthy();

    // 활성 브로커 수
    int count = checker.brokerCount();

    // 클러스터 ID
    String clusterId = checker.clusterId();

    // 컨트롤러 노드 정보
    Node controller = checker.controller();
}

// 타임아웃 지정 (기본 5초)
KafkaHealthChecker checker = new KafkaHealthChecker("localhost:9092", 3000L);
```

---

## 배포

### 1. GitHub Packages에 배포

`pom.xml`에 distributionManagement 추가:

```xml
<distributionManagement>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/{GITHUB_USERNAME}/kafka-message-utils</url>
    </repository>
</distributionManagement>
```

`~/.m2/settings.xml`에 인증 정보 추가:

```xml
<servers>
    <server>
        <id>github</id>
        <username>{GITHUB_USERNAME}</username>
        <password>{GITHUB_TOKEN}</password>  <!-- repo 권한 필요 -->
    </server>
</servers>
```

배포 실행:

```bash
mvn clean deploy
```

### 2. Maven Central에 배포

1. [Sonatype OSSRH](https://central.sonatype.org/) 계정 생성 및 groupId 소유권 확인
2. GPG 서명 설정

```bash
gpg --gen-key
gpg --list-secret-keys
```

3. `pom.xml`에 필수 메타데이터 추가 (name, description, url, licenses, developers, scm)
4. `maven-gpg-plugin`, `nexus-staging-maven-plugin` 추가
5. 배포

```bash
mvn clean deploy -P release
```

### 3. 팀 내부 Nexus / Artifactory

`pom.xml`의 `<distributionManagement>`에 내부 저장소 URL을 지정하고:

```bash
mvn clean deploy
```

### 버전 변경

```bash
mvn versions:set -DnewVersion=1.1.0
mvn versions:commit
```

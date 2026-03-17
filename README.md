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

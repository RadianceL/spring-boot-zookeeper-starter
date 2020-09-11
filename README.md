#spring-boot-zookeeper-starter

## quick start
```yaml
spring:
  el-util:
    zookeeper:
      namespace: switch
      base-sleep-time-ms: 20
      root-path: root
      server-addr: localhost:2181
      enable: true
      max-retries: 3
      digest: rt:rt
      sessionTimeoutMs: 50000
      connection-timeout-ms: 40000
```

```java
@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class SwitchServerConnector {

    private final ZookeeperRepository zookeeperRepository;
}
```
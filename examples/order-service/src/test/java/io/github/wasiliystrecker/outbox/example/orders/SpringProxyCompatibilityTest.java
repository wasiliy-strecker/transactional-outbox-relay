package io.github.wasiliystrecker.outbox.example.orders;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

final class SpringProxyCompatibilityTest {

  @Test
  void aopManagedComponentsRemainSubclassProxyable() {
    assertThat(Modifier.isFinal(OrderApplicationService.class.getModifiers())).isFalse();
    assertThat(Modifier.isFinal(OrderRepository.class.getModifiers())).isFalse();
  }
}

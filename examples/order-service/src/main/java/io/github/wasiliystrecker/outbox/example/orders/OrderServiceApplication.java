package io.github.wasiliystrecker.outbox.example.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Runnable portfolio example for atomic order and outbox writes. */
@SpringBootApplication
public class OrderServiceApplication {

  /** Creates the Spring Boot application container. */
  public OrderServiceApplication() {}

  /**
   * Starts the order service.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(OrderServiceApplication.class, args);
  }
}

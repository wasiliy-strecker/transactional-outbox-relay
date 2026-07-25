package io.github.wasiliystrecker.outbox.example.orders;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
final class OrderController {
  private final OrderApplicationService orders;

  OrderController(OrderApplicationService orders) {
    this.orders = orders;
  }

  @PostMapping
  ResponseEntity<OrderView> create(@Valid @RequestBody CreateOrderRequest request) {
    Order order =
        orders.create(
            new CreateOrderCommand(
                request.customerId(), request.totalAmount(), request.currency()));
    return ResponseEntity.created(URI.create("/api/orders/" + order.id()))
        .body(OrderView.from(order));
  }

  @GetMapping("/{id}")
  OrderView find(@PathVariable UUID id) {
    return OrderView.from(orders.find(id));
  }

  @ExceptionHandler(OrderNotFoundException.class)
  ProblemDetail notFound(OrderNotFoundException failure) {
    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, failure.getMessage());
    problem.setTitle("Order not found");
    return problem;
  }

  record CreateOrderRequest(
      @NotBlank @Size(max = 100) String customerId,
      @NotNull @DecimalMin("0.01") @Digits(integer = 12, fraction = 2) BigDecimal totalAmount,
      @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

  record OrderView(
      UUID id,
      String customerId,
      BigDecimal totalAmount,
      String currency,
      String status,
      long version,
      Instant createdAt) {

    static OrderView from(Order order) {
      return new OrderView(
          order.id(),
          order.customerId(),
          order.totalAmount(),
          order.currency(),
          order.status(),
          order.version(),
          order.createdAt());
    }
  }
}

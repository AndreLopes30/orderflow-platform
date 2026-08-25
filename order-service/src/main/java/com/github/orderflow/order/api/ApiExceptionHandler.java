package com.github.orderflow.order.api;

import com.github.orderflow.order.application.InvalidOrderStatusTransitionException;
import com.github.orderflow.order.application.OrderNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(OrderNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(OrderNotFoundException exception, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "Order not found", exception.getMessage(), "ORDER_NOT_FOUND", request);
    }

    @ExceptionHandler(InvalidOrderStatusTransitionException.class)
    ResponseEntity<ProblemDetail> handleInvalidTransition(
            InvalidOrderStatusTransitionException exception,
            HttpServletRequest request) {
        return problem(
                HttpStatus.CONFLICT,
                "Invalid order status transition",
                exception.getMessage(),
                "INVALID_STATUS_TRANSITION",
                request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> handleValidation(
            MethodArgumentNotValidException exception,
            HttpServletRequest request) {
        Map<String, String> errors = new LinkedHashMap<>();
        exception.getBindingResult().getFieldErrors()
                .forEach(error -> errors.putIfAbsent(error.getField(), error.getDefaultMessage()));
        ProblemDetail detail = createProblem(
                HttpStatus.BAD_REQUEST,
                "Request validation failed",
                "One or more fields are invalid",
                "VALIDATION_ERROR",
                request);
        detail.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(detail);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ProblemDetail> handleMalformedJson(
            HttpMessageNotReadableException exception,
            HttpServletRequest request) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "Malformed request",
                "Request body is missing or contains invalid JSON values",
                "MALFORMED_REQUEST",
                request);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ProblemDetail> handleTypeMismatch(
            MethodArgumentTypeMismatchException exception,
            HttpServletRequest request) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "Invalid path parameter",
                "Path parameter '" + exception.getName() + "' has an invalid value",
                "INVALID_PATH_PARAMETER",
                request);
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    ResponseEntity<ProblemDetail> handleConcurrentModification(
            ObjectOptimisticLockingFailureException exception,
            HttpServletRequest request) {
        return problem(
                HttpStatus.CONFLICT,
                "Concurrent order modification",
                "The order changed while this request was being processed; read it and retry",
                "CONCURRENT_MODIFICATION",
                request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> handleUnexpected(Exception exception, HttpServletRequest request) {
        log.atError()
                .addKeyValue("path", request.getRequestURI())
                .setCause(exception)
                .log("Unhandled request failure");
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal server error",
                "An unexpected error occurred",
                "INTERNAL_ERROR",
                request);
    }

    private ResponseEntity<ProblemDetail> problem(
            HttpStatus status,
            String title,
            String detail,
            String code,
            HttpServletRequest request) {
        return ResponseEntity.status(status).body(createProblem(status, title, detail, code, request));
    }

    private ProblemDetail createProblem(
            HttpStatus status,
            String title,
            String detail,
            String code,
            HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create("https://orderflow.local/problems/" + code.toLowerCase()));
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", code);
        problem.setProperty("timestamp", Instant.now());
        String traceId = MDC.get("traceId");
        if (traceId != null) {
            problem.setProperty("traceId", traceId);
        }
        return problem;
    }
}

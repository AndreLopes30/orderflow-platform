package com.github.orderflow.notification.api;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(NotificationNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(
            NotificationNotFoundException exception,
            HttpServletRequest request) {
        return problem(
                HttpStatus.NOT_FOUND,
                "Notification not found",
                exception.getMessage(),
                "NOTIFICATION_NOT_FOUND",
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
        return ResponseEntity.status(status).body(problem);
    }
}

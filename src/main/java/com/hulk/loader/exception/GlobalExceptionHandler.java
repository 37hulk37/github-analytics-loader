package com.hulk.loader.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ApplicationException.class)
    public ResponseEntity<RestException> catchException(ApplicationException e) {
        return ResponseEntity.badRequest()
            .body(new RestException(e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<RestException> catchException(Exception e) {
        log.error(e.getMessage(), e);
        return ResponseEntity.badRequest()
            .body(new RestException("Internal server error"));
    }

    public record RestException(String message) {
    }
}

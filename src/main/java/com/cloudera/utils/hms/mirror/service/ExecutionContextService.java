package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service that provides thread-local context for execution, allowing services
 * to retrieve the current thread's ConversionResult without passing it through
 * every method call.
 */
@Service
@Slf4j
public class ExecutionContextService {

    private final ThreadLocal<ConversionResult> conversionResultThreadLocal = new ThreadLocal<>();

    /**
     * Set the ConversionResult for the current thread.
     *
     * @param conversionResult the ConversionResult to associate with the current thread
     */
    public void setConversionResult(ConversionResult conversionResult) {
        log.debug("Setting ConversionResult for thread: {}", Thread.currentThread().getName());
        conversionResultThreadLocal.set(conversionResult);
    }

    /**
     * Get the ConversionResult for the current thread.
     *
     * @return the ConversionResult associated with the current thread, or null if not set
     */
    public ConversionResult getConversionResult() {
        ConversionResult result = conversionResultThreadLocal.get();
        if (result == null) {
            log.warn("No ConversionResult found for thread: {}", Thread.currentThread().getName());
        }
        return result;
    }

    /**
     * Clear the ConversionResult for the current thread.
     * This should be called when the execution context is no longer needed to prevent memory leaks.
     */
    public void clearConversionResult() {
        log.debug("Clearing ConversionResult for thread: {}", Thread.currentThread().getName());
        conversionResultThreadLocal.remove();
    }

    /**
     * Check if a ConversionResult is set for the current thread.
     *
     * @return true if a ConversionResult is set, false otherwise
     */
    public boolean hasConversionResult() {
        return conversionResultThreadLocal.get() != null;
    }
}

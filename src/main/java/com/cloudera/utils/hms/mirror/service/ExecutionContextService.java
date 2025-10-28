package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Service that provides thread-local context for execution, allowing services
 * to retrieve the current thread's ConversionResult without passing it through
 * every method call.
 */
@Service
@Slf4j
public class ExecutionContextService {

    private final ThreadLocal<ConversionResult> conversionResultThreadLocal = new ThreadLocal<>();
    private final ThreadLocal<RunStatus> runStatusThreadLocal = new ThreadLocal<>();
    private final ThreadLocal<HmsMirrorConfig> hmsMirrorConfigThreadLocal = new ThreadLocal<>();

    public void reset() {
        log.debug("Resetting execution context");
        conversionResultThreadLocal.remove();
        runStatusThreadLocal.remove();
        hmsMirrorConfigThreadLocal.remove();
    }

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
    public Optional<ConversionResult> getConversionResult() {
        ConversionResult result = conversionResultThreadLocal.get();
        if (result == null) {
            log.warn("No ConversionResult found for thread: {}", Thread.currentThread().getName());
        }
        return Optional.ofNullable(result);
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

    /**
     * Set the RunStatus for the current thread.
     *
     * @param runStatus the RunStatus to associate with the current thread
     */
    public void setRunStatus(RunStatus runStatus) {
        log.debug("Setting RunStatus for thread: {}", Thread.currentThread().getName());
        runStatusThreadLocal.set(runStatus);
    }

    /**
     * Get the RunStatus for the current thread.
     *
     * @return the RunStatus associated with the current thread, or null if not set
     */
    public Optional<RunStatus> getRunStatus() {
        RunStatus result = runStatusThreadLocal.get();
        if (result == null) {
            log.warn("No RunStatus found for thread: {}", Thread.currentThread().getName());
        }
        return Optional.ofNullable(result);
    }

    /**
     * Clear the RunStatus for the current thread.
     * This should be called when the execution context is no longer needed to prevent memory leaks.
     */
    public void clearRunStatus() {
        log.debug("Clearing RunStatus for thread: {}", Thread.currentThread().getName());
        runStatusThreadLocal.remove();
    }

    /**
     * Check if a RunStatus is set for the current thread.
     *
     * @return true if a RunStatus is set, false otherwise
     */
    public boolean hasRunStatus() {
        return runStatusThreadLocal.get() != null;
    }

    /**
     * Set the HmsMirrorConfig for the current thread.
     *
     * @param hmsMirrorConfig the HmsMirrorConfig to associate with the current thread
     */
    public void setHmsMirrorConfig(HmsMirrorConfig hmsMirrorConfig) {
        log.debug("Setting HmsMirrorConfig for thread: {}", Thread.currentThread().getName());
        hmsMirrorConfigThreadLocal.set(hmsMirrorConfig);
    }

    /**
     * Get the HmsMirrorConfig for the current thread.
     *
     * @return the HmsMirrorConfig associated with the current thread, or null if not set
     */
    public Optional<HmsMirrorConfig> getHmsMirrorConfig() {
        HmsMirrorConfig result = hmsMirrorConfigThreadLocal.get();
        if (result == null) {
            log.warn("No HmsMirrorConfig found for thread: {}", Thread.currentThread().getName());
        }
        return Optional.ofNullable(result);
    }

    /**
     * Clear the HmsMirrorConfig for the current thread.
     * This should be called when the execution context is no longer needed to prevent memory leaks.
     */
    public void clearHmsMirrorConfig() {
        log.debug("Clearing HmsMirrorConfig for thread: {}", Thread.currentThread().getName());
        hmsMirrorConfigThreadLocal.remove();
    }

    /**
     * Check if a HmsMirrorConfig is set for the current thread.
     *
     * @return true if a HmsMirrorConfig is set, false otherwise
     */
    public boolean hasHmsMirrorConfig() {
        return hmsMirrorConfigThreadLocal.get() != null;
    }

}

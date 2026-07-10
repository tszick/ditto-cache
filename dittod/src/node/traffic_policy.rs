use super::{CircuitState, NodeHandle};
use ditto_protocol::{ClientResponse, ErrorCode};
use std::{sync::atomic::Ordering, time::Instant};

#[derive(Debug)]
pub(super) struct TokenBucket {
    pub(super) capacity: f64,
    tokens: f64,
    pub(super) refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub(super) fn new(capacity: u64, refill_per_sec: u64) -> Self {
        let capacity = capacity.max(1) as f64;
        let refill_per_sec = refill_per_sec.max(1) as f64;
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec,
            last_refill: Instant::now(),
        }
    }

    pub(super) fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl NodeHandle {
    pub(super) fn allow_by_rate_limit(&self) -> bool {
        let (enabled, burst, requests_per_sec) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.rate_limit.enabled,
                cfg.rate_limit.burst.max(1),
                cfg.rate_limit.requests_per_sec.max(1),
            )
        };
        if !enabled {
            return true;
        }

        let mut bucket = self.rate_bucket.lock().unwrap();
        if (bucket.capacity as u64) != burst || (bucket.refill_per_sec as u64) != requests_per_sec {
            *bucket = TokenBucket::new(burst, requests_per_sec);
        }
        let allowed = bucket.try_take();
        if !allowed {
            self.rate_limited_requests_total
                .fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    pub(super) fn allow_by_circuit_breaker(&self) -> bool {
        let (enabled, half_open_max_requests) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.circuit_breaker.enabled,
                cfg.circuit_breaker.half_open_max_requests.max(1),
            )
        };
        if !enabled {
            return true;
        }
        let now_ms = Self::now_millis();
        let mut circuit = self.circuit.lock().unwrap();
        match circuit.state {
            CircuitState::Closed => true,
            CircuitState::Open if now_ms >= circuit.open_until_ms => {
                circuit.state = CircuitState::HalfOpen;
                circuit.half_open_successes = 0;
                true
            }
            CircuitState::Open => {
                self.circuit_breaker_reject_total
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
            CircuitState::HalfOpen if circuit.half_open_successes >= half_open_max_requests => {
                circuit.state = CircuitState::Closed;
                circuit.consecutive_failures = 0;
                circuit.half_open_successes = 0;
                true
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub(super) fn record_circuit_result(&self, response: &ClientResponse) {
        let (enabled, threshold, open_ms, half_open_max_requests) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.circuit_breaker.enabled,
                cfg.circuit_breaker.failure_threshold.max(1),
                cfg.circuit_breaker.open_ms.max(1),
                cfg.circuit_breaker.half_open_max_requests.max(1),
            )
        };
        if !enabled {
            return;
        }
        let failed = matches!(
            response,
            ClientResponse::Error {
                code: ErrorCode::WriteTimeout | ErrorCode::NoQuorum,
                ..
            }
        );
        let now_ms = Self::now_millis();
        let mut circuit = self.circuit.lock().unwrap();
        match circuit.state {
            CircuitState::Closed => {
                if failed {
                    circuit.consecutive_failures += 1;
                    if circuit.consecutive_failures >= threshold {
                        circuit.state = CircuitState::Open;
                        circuit.open_until_ms = now_ms.saturating_add(open_ms);
                        circuit.half_open_successes = 0;
                        self.circuit_breaker_open_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    circuit.consecutive_failures = 0;
                }
            }
            CircuitState::Open => {}
            CircuitState::HalfOpen if failed => {
                circuit.state = CircuitState::Open;
                circuit.open_until_ms = now_ms.saturating_add(open_ms);
                circuit.half_open_successes = 0;
                circuit.consecutive_failures = threshold;
                self.circuit_breaker_open_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                circuit.half_open_successes += 1;
                if circuit.half_open_successes >= half_open_max_requests {
                    circuit.state = CircuitState::Closed;
                    circuit.consecutive_failures = 0;
                    circuit.half_open_successes = 0;
                }
            }
        }
    }
}

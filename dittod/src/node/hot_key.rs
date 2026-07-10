use super::{GetFlight, HotKeyAdaptiveState, HotStaleEntry, NodeHandle};
use ditto_protocol::ClientResponse;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::watch;

impl NodeHandle {
    pub(super) fn execute_get(&self, lookup_key: String, response_key: String) -> ClientResponse {
        match self.store.get(&lookup_key) {
            Some(entry) => ClientResponse::Value {
                key: response_key,
                value: entry.value,
                version: entry.version,
            },
            None => ClientResponse::NotFound,
        }
    }

    fn is_hot_stale_candidate(response: &ClientResponse) -> bool {
        matches!(
            response,
            ClientResponse::Value { .. } | ClientResponse::NotFound
        )
    }

    pub(super) async fn read_hot_stale_response(
        &self,
        lookup_key: &str,
        stale_ttl_ms: u64,
    ) -> Option<ClientResponse> {
        if stale_ttl_ms == 0 {
            return None;
        }
        let now_ms = Self::now_millis();
        let mut stale = self.hot_key_stale_cache.lock().await;
        match stale.get(lookup_key) {
            Some(entry) if now_ms.saturating_sub(entry.recorded_at_ms) <= stale_ttl_ms => {
                Some(entry.response.clone())
            }
            Some(_) => {
                stale.remove(lookup_key);
                None
            }
            None => None,
        }
    }

    pub(super) async fn write_hot_stale_response(
        &self,
        lookup_key: &str,
        response: &ClientResponse,
        stale_ttl_ms: u64,
        stale_max_entries: usize,
    ) {
        if stale_ttl_ms == 0 || stale_max_entries == 0 || !Self::is_hot_stale_candidate(response) {
            return;
        }
        let mut stale = self.hot_key_stale_cache.lock().await;
        if stale.len() >= stale_max_entries && !stale.contains_key(lookup_key) {
            if let Some(oldest_key) = stale
                .iter()
                .min_by_key(|(_, entry)| entry.recorded_at_ms)
                .map(|(key, _)| key.clone())
            {
                stale.remove(&oldest_key);
            }
        }
        stale.insert(
            lookup_key.to_string(),
            HotStaleEntry {
                response: response.clone(),
                recorded_at_ms: Self::now_millis(),
            },
        );
    }

    pub(super) fn hot_key_adaptive_waiter_limit(
        &self,
        lookup_key: &str,
        enabled: bool,
        max_waiters: usize,
        adaptive_min_waiters: usize,
        adaptive_state_max_keys: usize,
    ) -> usize {
        if !enabled {
            return max_waiters;
        }
        let now_ms = Self::now_millis();
        let mut states = self.hot_key_adaptive_state.lock().unwrap();
        if !states.contains_key(lookup_key)
            && states.len() >= adaptive_state_max_keys.max(1)
            && adaptive_state_max_keys > 0
        {
            let oldest_key = states
                .iter()
                .min_by_key(|(_, v)| v.last_touched_ms)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest_key {
                states.remove(&k);
            }
        }

        let min_limit = adaptive_min_waiters.max(1).min(max_waiters.max(1));
        let state = states
            .entry(lookup_key.to_string())
            .or_insert(HotKeyAdaptiveState {
                limit: max_waiters.max(1),
                success_streak: 0,
                last_touched_ms: now_ms,
            });
        state.last_touched_ms = now_ms;
        state.limit = state.limit.clamp(min_limit, max_waiters.max(1));
        state.limit
    }

    pub(super) fn hot_key_adaptive_on_timeout(
        &self,
        lookup_key: &str,
        enabled: bool,
        max_waiters: usize,
        adaptive_min_waiters: usize,
    ) {
        if !enabled {
            return;
        }
        let now_ms = Self::now_millis();
        let min_limit = adaptive_min_waiters.max(1).min(max_waiters.max(1));
        let mut states = self.hot_key_adaptive_state.lock().unwrap();
        let state = states
            .entry(lookup_key.to_string())
            .or_insert(HotKeyAdaptiveState {
                limit: max_waiters.max(1),
                success_streak: 0,
                last_touched_ms: now_ms,
            });
        let old = state.limit.clamp(min_limit, max_waiters.max(1));
        let reduced = old.saturating_div(2).max(min_limit);
        state.limit = reduced;
        state.success_streak = 0;
        state.last_touched_ms = now_ms;
        if reduced < old {
            self.hot_key_adaptive_limit_decrease_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn hot_key_adaptive_on_success(
        &self,
        lookup_key: &str,
        enabled: bool,
        max_waiters: usize,
        adaptive_min_waiters: usize,
        adaptive_success_threshold: u32,
    ) {
        if !enabled {
            return;
        }
        let now_ms = Self::now_millis();
        let min_limit = adaptive_min_waiters.max(1).min(max_waiters.max(1));
        let threshold = adaptive_success_threshold.max(1);
        let mut states = self.hot_key_adaptive_state.lock().unwrap();
        let state = states
            .entry(lookup_key.to_string())
            .or_insert(HotKeyAdaptiveState {
                limit: max_waiters.max(1),
                success_streak: 0,
                last_touched_ms: now_ms,
            });
        state.limit = state.limit.clamp(min_limit, max_waiters.max(1));
        state.success_streak = state.success_streak.saturating_add(1);
        if state.success_streak >= threshold && state.limit < max_waiters.max(1) {
            state.limit = state.limit.saturating_add(1).min(max_waiters.max(1));
            state.success_streak = 0;
            self.hot_key_adaptive_limit_increase_total
                .fetch_add(1, Ordering::Relaxed);
        }
        state.last_touched_ms = now_ms;
    }

    pub(super) async fn handle_get_with_single_flight(
        &self,
        lookup_key: String,
        response_key: String,
    ) -> ClientResponse {
        let (
            enabled,
            max_waiters,
            follower_wait_timeout_ms,
            stale_ttl_ms,
            stale_max_entries,
            adaptive_waiters_enabled,
            adaptive_min_waiters,
            adaptive_success_threshold,
            adaptive_state_max_keys,
        ) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.hot_key.enabled,
                cfg.hot_key.max_waiters.max(1),
                cfg.hot_key.follower_wait_timeout_ms.max(1),
                cfg.hot_key.stale_ttl_ms,
                cfg.hot_key.stale_max_entries.max(1),
                cfg.hot_key.adaptive_waiters_enabled,
                cfg.hot_key.adaptive_min_waiters.max(1),
                cfg.hot_key.adaptive_success_threshold.max(1),
                cfg.hot_key.adaptive_state_max_keys.max(1),
            )
        };
        if !enabled {
            return self.execute_get(lookup_key, response_key);
        }

        enum JoinDecision {
            Leader(Arc<GetFlight>),
            Follower {
                flight: Arc<GetFlight>,
                rx: watch::Receiver<Option<ClientResponse>>,
            },
            Fallback,
        }

        let decision = {
            let mut flights = self.get_flights.lock().await;
            if let Some(existing) = flights.get(&lookup_key) {
                let current = existing.waiters.load(Ordering::Relaxed);
                let effective_limit = self.hot_key_adaptive_waiter_limit(
                    &lookup_key,
                    adaptive_waiters_enabled,
                    max_waiters,
                    adaptive_min_waiters,
                    adaptive_state_max_keys,
                );
                if current >= effective_limit {
                    JoinDecision::Fallback
                } else {
                    existing.waiters.fetch_add(1, Ordering::Relaxed);
                    let rx = existing.tx.subscribe();
                    JoinDecision::Follower {
                        flight: Arc::clone(existing),
                        rx,
                    }
                }
            } else {
                let created = Arc::new(GetFlight::new());
                flights.insert(lookup_key.clone(), Arc::clone(&created));
                JoinDecision::Leader(created)
            }
        };

        match decision {
            JoinDecision::Fallback => {
                if let Some(stale) = self
                    .read_hot_stale_response(&lookup_key, stale_ttl_ms)
                    .await
                {
                    self.hot_key_stale_served_total
                        .fetch_add(1, Ordering::Relaxed);
                    return stale;
                }
                self.hot_key_fallback_exec_total
                    .fetch_add(1, Ordering::Relaxed);
                let response = self.execute_get(lookup_key.clone(), response_key);
                self.write_hot_stale_response(
                    &lookup_key,
                    &response,
                    stale_ttl_ms,
                    stale_max_entries,
                )
                .await;
                response
            }
            JoinDecision::Leader(flight) => {
                let response = self.execute_get(lookup_key.clone(), response_key.clone());
                let _ = flight.tx.send(Some(response.clone()));
                let mut flights = self.get_flights.lock().await;
                if flights
                    .get(&lookup_key)
                    .map(|f| Arc::ptr_eq(f, &flight))
                    .unwrap_or(false)
                {
                    flights.remove(&lookup_key);
                }
                self.write_hot_stale_response(
                    &lookup_key,
                    &response,
                    stale_ttl_ms,
                    stale_max_entries,
                )
                .await;
                response
            }
            JoinDecision::Follower { flight, mut rx } => {
                self.hot_key_coalesced_hits_total
                    .fetch_add(1, Ordering::Relaxed);
                let response = if rx.borrow().is_some() {
                    rx.borrow().clone()
                } else {
                    match tokio::time::timeout(
                        Duration::from_millis(follower_wait_timeout_ms),
                        rx.changed(),
                    )
                    .await
                    {
                        Ok(Ok(())) => rx.borrow().clone(),
                        Ok(Err(_)) => None,
                        Err(_) => {
                            self.hot_key_wait_timeout_total
                                .fetch_add(1, Ordering::Relaxed);
                            self.hot_key_adaptive_on_timeout(
                                &lookup_key,
                                adaptive_waiters_enabled,
                                max_waiters,
                                adaptive_min_waiters,
                            );
                            None
                        }
                    }
                };
                flight.waiters.fetch_sub(1, Ordering::Relaxed);
                match response {
                    Some(resp) => {
                        self.hot_key_adaptive_on_success(
                            &lookup_key,
                            adaptive_waiters_enabled,
                            max_waiters,
                            adaptive_min_waiters,
                            adaptive_success_threshold,
                        );
                        resp
                    }
                    None => {
                        if let Some(stale) = self
                            .read_hot_stale_response(&lookup_key, stale_ttl_ms)
                            .await
                        {
                            self.hot_key_stale_served_total
                                .fetch_add(1, Ordering::Relaxed);
                            return stale;
                        }
                        self.hot_key_fallback_exec_total
                            .fetch_add(1, Ordering::Relaxed);
                        let response = self.execute_get(lookup_key.clone(), response_key);
                        self.write_hot_stale_response(
                            &lookup_key,
                            &response,
                            stale_ttl_ms,
                            stale_max_entries,
                        )
                        .await;
                        response
                    }
                }
            }
        }
    }
}

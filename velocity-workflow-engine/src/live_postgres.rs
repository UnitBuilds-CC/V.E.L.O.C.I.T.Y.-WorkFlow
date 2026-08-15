//! Live PostgreSQL persistence adapter using `tokio-postgres`.
//!
//! Runs a dedicated background thread with its own tokio runtime and a
//! `tokio-postgres` client.  The synchronous [`DatabaseAdapter`] trait methods
//! send work to the background thread via a channel — this avoids the
//! "cannot start a runtime from within a runtime" panic that would occur
//! if we used `Handle::block_on()` from a tokio worker thread.
//!
//! Enable with the `postgres` feature flag on `velocity-workflow-engine`.

#[cfg(feature = "postgres")]
mod inner {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    use tokio::runtime::Runtime;
    use tokio::sync::Mutex as TokioMutex;
    use tokio_postgres::Client;

    use crate::db_adapter::{
        DatabaseAdapter, DatabaseConfig, DatabaseError, DatabaseResult, SearchAttributeValue,
        SearchAttributes, StatusFilter, WorkflowEventRecord, WorkflowRecord,
    };
    use crate::engine::WorkflowStatus;

    // ─── Channel-based work dispatch ──────────────────────────────────────

    type BoxFuture = Box<dyn FnOnce(Arc<TokioMutex<Client>>) -> std::pin::Pin<Box<dyn std::future::Future<Output = DatabaseResult<()>> + Send>> + Send>;
    type SaveFuture = Box<dyn FnOnce(Arc<TokioMutex<Client>>) -> std::pin::Pin<Box<dyn std::future::Future<Output = DatabaseResult<u64>> + Send>> + Send>;

    enum PgTask {
        Save {
            task: BoxFuture,
            reply: std::sync::mpsc::Sender<DatabaseResult<()>>,
        },
        SaveWithId {
            task: SaveFuture,
            reply: std::sync::mpsc::Sender<DatabaseResult<u64>>,
        },
        Shutdown,
    }

    /// A real PostgreSQL-backed [`DatabaseAdapter`] that uses `tokio-postgres`
    /// on a dedicated background thread, communicating via channels.
    pub struct LivePostgresAdapter {
        tx: std::sync::mpsc::Sender<PgTask>,
        connected: Arc<AtomicBool>,
        _bg: thread::JoinHandle<()>,
    }

    impl LivePostgresAdapter {
        /// Connect to PostgreSQL, initialize schema, and seed the default namespace.
        pub fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
            let conn_str = config.to_connection_string();
            let connected = Arc::new(AtomicBool::new(false));
            let connected_clone = connected.clone();

            let (ready_tx, ready_rx) =
                std::sync::mpsc::channel::<Result<(), DatabaseError>>();
            let (task_tx, task_rx) = std::sync::mpsc::channel::<PgTask>();

            let bg = thread::Builder::new()
                .name("velocity-pg".into())
                .spawn(move || {
                    let rt = match Runtime::new() {
                        Ok(rt) => rt,
                        Err(e) => {
                            let _ = ready_tx.send(Err(DatabaseError::ConnectionError(
                                format!("runtime: {}", e),
                            )));
                            return;
                        }
                    };

                    rt.block_on(async {
                        let (client, connection) =
                            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                                Ok(pair) => pair,
                                Err(e) => {
                                    let _ = ready_tx.send(Err(DatabaseError::ConnectionError(
                                        e.to_string(),
                                    )));
                                    return;
                                }
                            };

                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("velocity-pg connection error: {}", e);
                            }
                        });

                        // Initialize schema — check if already created.
                        let tables_exist = client
                            .query_opt(
                                "SELECT 1 FROM information_schema.tables WHERE table_name = 'workflows'",
                                &[],
                            )
                            .await
                            .ok()
                            .flatten()
                            .is_some();

                        if !tables_exist {
                            if let Err(e) =
                                client.batch_execute(crate::db_adapter::SCHEMA_SQL).await
                            {
                                let retry = client
                                    .query_opt(
                                        "SELECT 1 FROM information_schema.tables WHERE table_name = 'workflows'",
                                        &[],
                                    )
                                    .await
                                    .ok()
                                    .flatten()
                                    .is_some();
                                if !retry {
                                    let _ = ready_tx.send(Err(DatabaseError::SchemaError(
                                        e.to_string(),
                                    )));
                                    return;
                                }
                            }
                        }

                        // Ensure step_journal table exists (handles upgrade from
                        // older schema that didn't have it).
                        let journal_exists = client
                            .query_opt(
                                "SELECT 1 FROM information_schema.tables WHERE table_name = 'step_journal'",
                                &[],
                            )
                            .await
                            .ok()
                            .flatten()
                            .is_some();
                        if !journal_exists {
                            let _ = client.batch_execute(
                                "CREATE TABLE IF NOT EXISTS step_journal (\
                                    id BIGSERIAL PRIMARY KEY,\
                                    workflow_key BIGINT NOT NULL,\
                                    step_number INTEGER NOT NULL,\
                                    result_data BYTEA,\
                                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
                                );\
                                CREATE INDEX IF NOT EXISTS idx_step_journal_workflow ON step_journal (workflow_key);"
                            ).await;
                        }

                        // Seed default namespace.
                        let _ = client
                            .execute(
                                "INSERT INTO namespaces (name, display_name) \
                                 VALUES ('default', 'Default') ON CONFLICT DO NOTHING",
                                &[],
                            )
                            .await;

                        connected_clone.store(true, Ordering::SeqCst);
                        let _ = ready_tx.send(Ok(()));

                        // Process tasks from the channel.
                        // Wrap client in Arc<Mutex> so we can share it across task boundaries.
                        let client = Arc::new(tokio::sync::Mutex::new(client));

                        while let Ok(task) = task_rx.recv() {
                            match task {
                                PgTask::Save { task, reply } => {
                                    let result = task(client.clone()).await;
                                    let _ = reply.send(result);
                                }
                                PgTask::SaveWithId { task, reply } => {
                                    let result = task(client.clone()).await;
                                    let _ = reply.send(result);
                                }
                                PgTask::Shutdown => break,
                            }
                        }
                    });
                })
                .map_err(|e| {
                    DatabaseError::ConnectionError(format!("spawn pg thread: {}", e))
                })?;

            ready_rx
                .recv()
                .map_err(|_| {
                    DatabaseError::ConnectionError("pg thread died during init".into())
                })??;

            Ok(Self {
                tx: task_tx,
                connected,
                _bg: bg,
            })
        }

        /// Send a task that returns () to the background thread and wait for the result.
        fn run_task(
            &self,
            task: impl FnOnce(Arc<TokioMutex<Client>>) -> std::pin::Pin<Box<dyn std::future::Future<Output = DatabaseResult<()>> + Send>> + Send + 'static,
        ) -> DatabaseResult<()> {
            let (reply_tx, reply_rx) = std::sync::mpsc::channel();
            self.tx
                .send(PgTask::Save {
                    task: Box::new(task),
                    reply: reply_tx,
                })
                .map_err(|_| DatabaseError::NotConnected)?;
            reply_rx
                .recv()
                .map_err(|_| DatabaseError::NotConnected)?
        }

        /// Send a task that returns u64 to the background thread and wait for the result.
        fn run_task_u64(
            &self,
            task: impl FnOnce(Arc<TokioMutex<Client>>) -> std::pin::Pin<Box<dyn std::future::Future<Output = DatabaseResult<u64>> + Send>> + Send + 'static,
        ) -> DatabaseResult<u64> {
            let (reply_tx, reply_rx) = std::sync::mpsc::channel();
            self.tx
                .send(PgTask::SaveWithId {
                    task: Box::new(task),
                    reply: reply_tx,
                })
                .map_err(|_| DatabaseError::NotConnected)?;
            reply_rx
                .recv()
                .map_err(|_| DatabaseError::NotConnected)?
        }
    }

    // ─── DatabaseAdapter implementation ─────────────────────────────────────

    impl DatabaseAdapter for LivePostgresAdapter {
        fn init_schema(&self) -> DatabaseResult<()> {
            Ok(()) // done in new()
        }

        fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()> {
            let step_results = serde_json::to_value(
                &record
                    .step_results
                    .iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(hex_encode(v))))
                    .collect::<HashMap<_, _>>(),
            )
            .unwrap_or(serde_json::json!({}));
            let signal_buffer =
                serde_json::to_value(&record.signal_buffer).unwrap_or(serde_json::json!({}));
            let update_buffer =
                serde_json::to_value(&record.update_buffer).unwrap_or(serde_json::json!({}));
            let child_keys: Vec<i64> = record.child_keys.iter().map(|&k| k as i64).collect();
            let status_code = status_to_i16(record.status);

            let k = key as i64;
            let wf_id = record.workflow_id as i64;
            let run_id = record.run_id as i64;
            let wf_type = record.workflow_type_id as i64;
            let ns_id = record.namespace_id as i64;
            let ns_name = record.namespace_name.clone();
            let tq = record.task_queue_hash as i64;
            let cur = record.current_step as i32;
            let tot = record.total_steps as i32;
            let seq = record.event_sequence as i64;
            let parent = record.parent_key.map(|p| p as i64);
            let merkle = record.merkle_root.clone();
            let bitmask = record.step_bitmask.clone();
            let input = record.input_data.clone();
            let result = record.result_data.clone();

            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    c
                        .execute(
                            crate::db_adapter::sql::UPSERT_WORKFLOW,
                            &[
                                &k, &wf_id, &run_id, &wf_type, &ns_id,
                                &ns_name.as_str(),
                                &tq, &cur, &tot,
                                &merkle.as_slice(), &bitmask.as_slice(),
                                &status_code,
                                &step_results, &signal_buffer, &update_buffer,
                                &input, &result,
                                &parent, &child_keys,
                                &seq,
                                &1i32,
                            ],
                        )
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(())
                })
            })
        }

        fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord> {
            let k = key as i64;
            let result_arc = Arc::new(std::sync::Mutex::new(None));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let row = c
                        .query_one(crate::db_adapter::sql::SELECT_WORKFLOW, &[&k])
                        .await
                        .map_err(|_| DatabaseError::NotFound(key))?;
                    let record = row_to_record(&row);
                    *result_arc.lock().unwrap() = Some(record);
                    Ok(())
                })
            })?;

            let record = result_arc.lock().unwrap().take();
            record.ok_or_else(|| DatabaseError::NotFound(key))
        }

        fn delete_workflow(&self, key: u64) -> DatabaseResult<()> {
            let k = key as i64;
            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    c.execute(crate::db_adapter::sql::DELETE_WORKFLOW, &[&k])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(())
                })
            })
        }

        fn list_workflows(
            &self,
            namespace: Option<&str>,
            status_filter: StatusFilter,
            limit: u32,
            offset: u32,
        ) -> DatabaseResult<Vec<WorkflowRecord>> {
            let ns = namespace.map(|s| s.to_string());
            let sc = status_filter.to_status_code();
            let lim = limit as i32;
            let off = offset as i32;
            let result_arc = Arc::new(std::sync::Mutex::new(Vec::new()));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let rows = c
                        .query(
                            crate::db_adapter::sql::LIST_WORKFLOWS,
                            &[&ns, &sc, &lim, &off],
                        )
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    *result_arc.lock().unwrap() = rows.iter().map(row_to_record).collect();
                    Ok(())
                })
            })?;

            let data = result_arc.lock().unwrap().clone();
            Ok(data)
        }

        fn save_event(
            &self,
            workflow_key: u64,
            event_type: u8,
            event_type_name: &str,
            sequence_num: u64,
            data: Vec<u8>,
        ) -> DatabaseResult<i64> {
            let wk = workflow_key as i64;
            let et = event_type as i16;
            let etn = event_type_name.to_string();
            let sn = sequence_num as i64;
            let result_arc = Arc::new(std::sync::Mutex::new(0i64));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let row = c
                        .query_one(
                            crate::db_adapter::sql::INSERT_EVENT,
                            &[&wk, &et, &etn, &sn, &data],
                        )
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    *result_arc.lock().unwrap() = row.get::<_, i64>(0);
                    Ok(())
                })
            })?;

            let val = *result_arc.lock().unwrap();
            Ok(val)
        }

        fn load_events(&self, workflow_key: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> {
            let wk = workflow_key as i64;
            let result_arc = Arc::new(std::sync::Mutex::new(Vec::new()));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let rows = c
                        .query(crate::db_adapter::sql::SELECT_EVENTS, &[&wk])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    *result_arc.lock().unwrap() = rows
                        .iter()
                        .map(|r| WorkflowEventRecord {
                            id: r.get(0),
                            workflow_key: r.get::<_, i64>(1) as u64,
                            event_type: r.get::<_, i16>(2) as u8,
                            event_type_name: r.get(3),
                            sequence_num: r.get::<_, i64>(4) as u64,
                            data: r.get::<_, Option<Vec<u8>>>(5).unwrap_or_default(),
                            metadata: HashMap::new(),
                        })
                        .collect();
                    Ok(())
                })
            })?;

            let data = result_arc.lock().unwrap().clone();
            Ok(data)
        }

        fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()> {
            let k = key as i64;
            let entries: Vec<(String, SearchAttributeValue)> = attrs
                .iter()
                .map(|(n, v)| (n.clone(), v.clone()))
                .collect();

            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    c.execute(crate::db_adapter::sql::DELETE_SEARCH_ATTRS, &[&k])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    for (name, val) in &entries {
                        let tc = val.type_code();
                        match val {
                            SearchAttributeValue::Text(s) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &Some(s.as_str()), &None::<i64>, &None::<f64>, &None::<bool>, &None::<&str>, &None::<&[u8]>, &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::Integer(i) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &Some(*i), &None::<f64>, &None::<bool>, &None::<&str>, &None::<&[u8]>, &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::Float(f) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &Some(*f), &None::<bool>, &None::<&str>, &None::<&[u8]>, &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::Bool(b) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &None::<f64>, &Some(*b), &None::<&str>, &None::<&[u8]>, &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::Bytes(b) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &None::<f64>, &None::<bool>, &None::<&str>, &Some(b.as_slice()), &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::TextArray(a) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &None::<f64>, &None::<bool>, &None::<&str>, &None::<&[u8]>, &Some(a.clone()), &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::IntArray(a) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &None::<f64>, &None::<bool>, &None::<&str>, &None::<&[u8]>, &None::<Vec<String>>, &Some(a.clone())])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                            SearchAttributeValue::DateTime(s) => {
                                c.execute(crate::db_adapter::sql::UPSERT_SEARCH_ATTR,
                                    &[&k, &name.as_str(), &tc, &None::<&str>, &None::<i64>, &None::<f64>, &None::<bool>, &Some(s.as_str()), &None::<&[u8]>, &None::<Vec<String>>, &None::<Vec<i64>>])
                                    .await.map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                            }
                        }
                    }
                    Ok(())
                })
            })
        }

        fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes> {
            let k = key as i64;
            let result_arc = Arc::new(std::sync::Mutex::new(HashMap::new()));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let rows = c
                        .query(crate::db_adapter::sql::SELECT_SEARCH_ATTRS, &[&k])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    let mut attrs = HashMap::new();
                    for row in &rows {
                        let name: String = row.get(0);
                        let tc: i16 = row.get(1);
                        let val = match tc {
                            0 => SearchAttributeValue::Text(
                                row.get::<_, Option<String>>(2).unwrap_or_default(),
                            ),
                            1 => SearchAttributeValue::Integer(
                                row.get::<_, Option<i64>>(3).unwrap_or(0),
                            ),
                            2 => SearchAttributeValue::Float(
                                row.get::<_, Option<f64>>(4).unwrap_or(0.0),
                            ),
                            3 => SearchAttributeValue::Bool(
                                row.get::<_, Option<bool>>(5).unwrap_or(false),
                            ),
                            5 => SearchAttributeValue::Bytes(
                                row.get::<_, Option<Vec<u8>>>(7).unwrap_or_default(),
                            ),
                            _ => SearchAttributeValue::Text(
                                row.get::<_, Option<String>>(2).unwrap_or_default(),
                            ),
                        };
                        attrs.insert(name, val);
                    }
                    *result_arc.lock().unwrap() = attrs;
                    Ok(())
                })
            })?;

            let data = result_arc.lock().unwrap().clone();
            Ok(data)
        }

        fn update_workflow_status(
            &self,
            key: u64,
            status: WorkflowStatus,
        ) -> DatabaseResult<()> {
            let k = key as i64;
            let s = status_to_i16(status);
            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    c.execute(crate::db_adapter::sql::UPDATE_STATUS, &[&k, &s])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(())
                })
            })
        }

        fn count_workflows(
            &self,
            namespace: Option<&str>,
            status_filter: StatusFilter,
        ) -> DatabaseResult<u64> {
            let ns = namespace.map(|s| s.to_string());
            let sc = status_filter.to_status_code();
            let result_arc = Arc::new(std::sync::Mutex::new(0u64));
            let result_clone = result_arc.clone();

            self.run_task(move |client_arc| {
                let result_arc = result_clone;
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let row = c
                        .query_one(crate::db_adapter::sql::COUNT_WORKFLOWS, &[&ns, &sc])
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    *result_arc.lock().unwrap() = row.get::<_, i64>(0) as u64;
                    Ok(())
                })
            })?;

            let val = *result_arc.lock().unwrap();
            Ok(val)
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }

        fn adapter_name(&self) -> &str {
            "LivePostgresAdapter"
        }

        fn save_step(&self, workflow_key: u64, step_number: u32, result_data: Option<&[u8]>) -> DatabaseResult<()> {
            let wk = workflow_key as i64;
            let sn = step_number as i32;
            let rd = result_data.map(|d| d.to_vec());

            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    c
                        .execute(
                            crate::db_adapter::sql::APPEND_STEP,
                            &[&wk, &sn, &rd],
                        )
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(())
                })
            })
        }

        fn save_steps_batch(&self, workflow_key: u64, steps: &[(u32, Option<Vec<u8>>)]) -> DatabaseResult<()> {
            if steps.is_empty() {
                return Ok(());
            }

            // Build a multi-row INSERT: VALUES ($1,$2,$3), ($4,$5,$6), ...
            let wk = workflow_key as i64;
            let mut sql = String::from("INSERT INTO step_journal (workflow_key, step_number, result_data) VALUES ");
            let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::with_capacity(steps.len() * 3);
            for (i, (step_num, result)) in steps.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let base = i * 3;
                sql.push_str(&format!(
                    "(${}, ${}, ${})",
                    base + 1, base + 2, base + 3
                ));
                params.push(Box::new(wk));
                params.push(Box::new(*step_num as i32));
                params.push(Box::new(result.clone()));
            }

            self.run_task(move |client_arc| {
                Box::pin(async move {
                    let client = client_arc.lock_owned().await;
                    let c = &*client;
                    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                        params.iter().map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                    c.execute(&sql, &param_refs)
                        .await
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(())
                })
            })
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    fn status_to_i16(s: WorkflowStatus) -> i16 {
        match s {
            WorkflowStatus::Void => 0,
            WorkflowStatus::Running => 1,
            WorkflowStatus::Completed => 2,
            WorkflowStatus::Failed => 3,
            WorkflowStatus::Canceled => 4,
            WorkflowStatus::Terminated => 5,
            WorkflowStatus::ContinuedAsNew => 6,
            WorkflowStatus::TimedOut => 7,
        }
    }

    fn i16_to_status(code: i16) -> WorkflowStatus {
        match code {
            1 => WorkflowStatus::Running,
            2 => WorkflowStatus::Completed,
            3 => WorkflowStatus::Failed,
            4 => WorkflowStatus::Canceled,
            5 => WorkflowStatus::Terminated,
            6 => WorkflowStatus::ContinuedAsNew,
            7 => WorkflowStatus::TimedOut,
            _ => WorkflowStatus::Void,
        }
    }

    fn row_to_record(row: &tokio_postgres::Row) -> WorkflowRecord {
        let sr: serde_json::Value = row.get(12);
        let sb: serde_json::Value = row.get(13);
        let ub: serde_json::Value = row.get(14);

        WorkflowRecord {
            workflow_key: row.get::<_, i64>(0) as u64,
            workflow_id: row.get::<_, i64>(1) as u64,
            run_id: row.get::<_, i64>(2) as u64,
            workflow_type_id: row.get::<_, i64>(3) as u64,
            namespace_id: row.get::<_, i64>(4) as u64,
            namespace_name: row.get(5),
            task_queue_hash: row.get::<_, i64>(6) as u64,
            current_step: row.get::<_, i32>(7) as u32,
            total_steps: row.get::<_, i32>(8) as u32,
            merkle_root: row.get::<_, Vec<u8>>(9),
            step_bitmask: row.get::<_, Vec<u8>>(10),
            status: i16_to_status(row.get::<_, i16>(11)),
            step_results: if let serde_json::Value::Object(map) = sr {
                map.iter()
                    .filter_map(|(k, v)| {
                        let step: u32 = k.parse().ok()?;
                        let bytes = if let serde_json::Value::String(hex) = v {
                            hex_decode(hex)
                        } else {
                            Vec::new()
                        };
                        Some((step, bytes))
                    })
                    .collect()
            } else {
                HashMap::new()
            },
            signal_buffer: serde_json::from_value(sb).unwrap_or_default(),
            update_buffer: serde_json::from_value(ub).unwrap_or_default(),
            input_data: row.get(15),
            result_data: row.get(16),
            parent_key: row.get::<_, Option<i64>>(17).map(|v| v as u64),
            child_keys: row.get::<_, Vec<i64>>(18).iter().map(|&k| k as u64).collect(),
            event_sequence: row.get::<_, i64>(19) as u64,
        }
    }

    fn hex_encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
            .collect()
    }
}

#[cfg(feature = "postgres")]
pub use inner::LivePostgresAdapter;

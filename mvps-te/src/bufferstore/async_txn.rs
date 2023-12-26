use std::{collections::HashMap, ops::Deref, rc::Rc, sync::atomic::AtomicU64};

use futures::future::Either;
use heed::{Env, RoTxn, RwTxn};
use once_cell::sync::Lazy;
use rand::Rng;
use tokio::sync::{
  mpsc::{UnboundedReceiver, UnboundedSender},
  oneshot,
};

static ASYNC_TXN_THREAD: Lazy<Vec<UnboundedSender<Task>>> = Lazy::new(|| {
  (0..8)
    .map(|i| {
      let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
      std::thread::Builder::new()
        .name(format!("asynctxn-{}", i))
        .spawn(move || {
          tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async_txn_thread(rx));
        })
        .unwrap();
      tx
    })
    .collect::<Vec<_>>()
});

static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

struct AsyncTxn {
  thread_id: usize,
  txn_id: u64,
  completed: bool,
}

pub struct AsyncRwTxn {
  txn: AsyncRoTxn,
}

pub struct AsyncRoTxn {
  txn: AsyncTxn,
}

impl AsyncTxn {
  async fn begin(env: Env, write: bool) -> anyhow::Result<Self> {
    let (tx, rx) = oneshot::channel();
    let thread_id = rand::thread_rng().gen_range(0..ASYNC_TXN_THREAD.len());
    let txn_id = NEXT_TXN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ASYNC_TXN_THREAD[thread_id]
      .send(Task::Begin {
        id: txn_id,
        write,
        env,
        tx,
      })
      .unwrap();
    let me = Self {
      thread_id,
      txn_id,
      completed: false,
    };
    rx.await?;
    Ok(me)
  }
}

impl Drop for AsyncTxn {
  fn drop(&mut self) {
    if !self.completed {
      let (tx, _) = oneshot::channel();
      ASYNC_TXN_THREAD[self.thread_id]
        .send(Task::Complete {
          id: self.txn_id,
          commit: false,
          tx,
        })
        .unwrap();
    }
  }
}

impl AsyncRwTxn {
  pub async fn begin(env: Env) -> anyhow::Result<Self> {
    Ok(Self {
      txn: AsyncRoTxn {
        txn: AsyncTxn::begin(env, true).await?,
      },
    })
  }

  pub async fn commit(mut self) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    ASYNC_TXN_THREAD[self.txn.txn.thread_id]
      .send(Task::Complete {
        id: self.txn.txn.txn_id,
        commit: true,
        tx,
      })
      .unwrap();
    self.txn.txn.completed = true;
    rx.await?;
    Ok(())
  }

  pub async fn _abort(mut self) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    ASYNC_TXN_THREAD[self.txn.txn.thread_id]
      .send(Task::Complete {
        id: self.txn.txn.txn_id,
        commit: false,
        tx,
      })
      .unwrap();
    self.txn.txn.completed = true;
    rx.await?;
    Ok(())
  }

  pub async fn with<F, R>(&self, callback: F) -> R
  where
    F: for<'a> FnOnce(&mut RwTxn<'a, 'a, ()>) -> R + Send + 'static,
    R: Send + 'static,
  {
    let (tx, rx) = oneshot::channel();
    ASYNC_TXN_THREAD[self.txn.txn.thread_id]
      .send(Task::Rw(
        self.txn.txn.txn_id,
        Box::new(move |txn| {
          let ret = callback(txn);
          let _ = tx.send(ret);
        }),
      ))
      .unwrap();
    rx.await.unwrap()
  }
}

impl Deref for AsyncRwTxn {
  type Target = AsyncRoTxn;

  fn deref(&self) -> &Self::Target {
    &self.txn
  }
}

impl AsyncRoTxn {
  pub async fn begin(env: Env) -> anyhow::Result<Self> {
    Ok(Self {
      txn: AsyncTxn::begin(env, false).await?,
    })
  }

  pub async fn with<F, R>(&self, callback: F) -> R
  where
    F: for<'a> FnOnce(&RoTxn<'a, ()>) -> R + Send + 'static,
    R: Send + 'static,
  {
    let (tx, rx) = oneshot::channel();
    ASYNC_TXN_THREAD[self.txn.thread_id]
      .send(Task::Ro(
        self.txn.txn_id,
        Box::new(move |txn| {
          let ret = callback(txn);
          let _ = tx.send(ret);
        }),
      ))
      .unwrap();
    rx.await.unwrap()
  }
}

enum Task {
  Begin {
    id: u64,
    write: bool,
    env: Env,
    tx: oneshot::Sender<()>,
  },
  Rw(
    u64,
    Box<dyn for<'a> FnOnce(&mut RwTxn<'a, 'a, ()>) + Send + 'static>,
  ),
  Ro(
    u64,
    Box<dyn for<'a> FnOnce(&RoTxn<'a, ()>) + Send + 'static>,
  ),
  Complete {
    id: u64,
    commit: bool,
    tx: oneshot::Sender<()>,
  },
}

async fn async_txn_thread(mut rx: UnboundedReceiver<Task>) {
  let mut txns: HashMap<
    u64,
    (
      Rc<Env>,
      Either<RwTxn<'static, 'static, ()>, RoTxn<'static, ()>>,
    ),
  > = HashMap::new();

  loop {
    let task = rx.recv().await.unwrap();

    match task {
      Task::Begin { id, write, env, tx } => {
        let env = Rc::new(env);
        let txn = if write {
          env.write_txn().map(Either::Left)
        } else {
          env.read_txn().map(Either::Right)
        };
        match txn {
          Ok(x) => {
            txns.insert(
              id,
              (env.clone(), unsafe {
                std::mem::transmute::<
                  Either<RwTxn<'_, '_, ()>, RoTxn<'_, ()>>,
                  Either<RwTxn<'static, 'static, ()>, RoTxn<'static, ()>>,
                >(x)
              }),
            );
            let _ = tx.send(());
          }
          Err(e) => {
            tracing::error!(error = ?e, write, "failed to begin txn");
            drop(tx);
          }
        }
      }
      Task::Rw(id, callback) => {
        let (_env, txn) = txns.get_mut(&id).unwrap();
        match txn {
          Either::Left(txn) => callback(txn),
          Either::Right(_) => unreachable!(),
        }
      }
      Task::Ro(id, callback) => {
        let (_env, txn) = txns.get_mut(&id).unwrap();
        match txn {
          Either::Left(txn) => callback(&**txn),
          Either::Right(txn) => callback(txn),
        }
      }
      Task::Complete { id, commit, tx } => {
        let Some((env, txn)) = txns.remove(&id) else {
          continue;
        };

        // Ensure `txn` is dropped before `env`
        {
          let txn = txn;

          match txn {
            Either::Left(txn) => match if commit { txn.commit() } else { txn.abort() } {
              Ok(()) => {
                let _ = tx.send(());
              }
              Err(e) => {
                tracing::error!(error = ?e, commit, "failed to complete read-write txn");
                drop(tx);
              }
            },
            Either::Right(txn) => match txn.abort() {
              Ok(()) => {
                let _ = tx.send(());
              }
              Err(e) => {
                tracing::error!(error = ?e, "failed to complete read-only txn");
                drop(tx)
              }
            },
          }
        }

        drop(env);
      }
    }
  }
}

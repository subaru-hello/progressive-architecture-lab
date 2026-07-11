import { Worker } from 'node:worker_threads';

interface Job {
  id: number;
  payload: { cpu: number };
  resolve: (value: { sum: number }) => void;
  reject: (reason: unknown) => void;
}

interface WorkerState {
  worker: Worker;
  currentJobId: number | null;
}

export class WorkerPool {
  private readonly workers: WorkerState[];
  private readonly queue: Job[] = [];
  private readonly pending = new Map<number, Job>();
  private readonly workerPath: URL;
  private nextId = 0;
  private destroyed = false;

  constructor(size: number, workerPath: URL) {
    this.workerPath = workerPath;
    this.workers = Array.from({ length: size }, () => {
      const state: WorkerState = { worker: null as unknown as Worker, currentJobId: null };
      this._spawn(state);
      return state;
    });
  }

  private _spawn(state: WorkerState): void {
    const worker = new Worker(this.workerPath);
    state.worker = worker;

    worker.on('message', (msg: { id: number; sum: number }) => {
      const job = this.pending.get(msg.id);
      if (job) {
        this.pending.delete(msg.id);
        state.currentJobId = null;
        job.resolve({ sum: msg.sum });
        this._drain(state);
      }
    });

    // A worker can die two ways: 'error' (an uncaught throw, followed by 'exit'),
    // or 'exit' alone (process.exit / OOM kill / terminate). Either way its in-flight
    // job must be rejected — leaving it pending would hang the /work request forever —
    // and the worker replaced so the pool self-heals. This lab deliberately burns CPU
    // hard enough to kill workers, so this recovery path is load-bearing.
    worker.on('error', (err) => this._handleDeath(state, err));
    worker.on('exit', (code) => {
      if (code !== 0) this._handleDeath(state, new Error(`worker exited with code ${code}`));
    });
  }

  // Idempotent-safe: 'error' then 'exit' both fire for an uncaught throw, but the
  // first call removes the dead worker's listeners so the second never runs.
  private _handleDeath(state: WorkerState, reason: unknown): void {
    if (state.currentJobId !== null) {
      const job = this.pending.get(state.currentJobId);
      if (job) {
        this.pending.delete(state.currentJobId);
        job.reject(reason);
      }
      state.currentJobId = null;
    }
    if (this.destroyed) return;
    state.worker.removeAllListeners();
    this._spawn(state); // replace the dead worker in place
    this._drain(state); // feed it any backlog that piled up
  }

  run(payload: { cpu: number }): Promise<{ sum: number }> {
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      const job: Job = { id, payload, resolve, reject };

      const free = this.workers.find((s) => s.currentJobId === null);
      if (free) {
        this._assign(free, job);
      } else {
        this.queue.push(job);
      }
    });
  }

  get queueDepth(): number {
    return this.queue.length;
  }

  get busy(): number {
    return this.workers.filter((s) => s.currentJobId !== null).length;
  }

  async destroy(): Promise<void> {
    this.destroyed = true; // stop _handleDeath from respawning on terminate's exit
    await Promise.all(this.workers.map((s) => s.worker.terminate()));
  }

  private _assign(state: WorkerState, job: Job): void {
    state.currentJobId = job.id;
    this.pending.set(job.id, job);
    state.worker.postMessage({ id: job.id, cpu: job.payload.cpu });
  }

  private _drain(state: WorkerState): void {
    if (this.queue.length > 0) {
      const next = this.queue.shift()!;
      this._assign(state, next);
    }
  }
}

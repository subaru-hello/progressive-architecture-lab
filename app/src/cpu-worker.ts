import { parentPort } from 'node:worker_threads';

parentPort!.on('message', ({ id, cpu }: { id: number; cpu: number }) => {
  let sum = 0;
  for (let i = 0; i < cpu * 1_000_000; i++) sum += Math.sqrt(i);
  parentPort!.postMessage({ id, sum });
});

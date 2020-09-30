import fs from 'fs-extra';

import { backOff } from './back-off';
import { QUEUE_PERSIST_FOLDER } from './environment';

class RequestQueue {

  constructor(id, requests = []) {
    this.id = id;
    this.requests = requests;
    this.busy = false;
    this.error = null;
    this.dequeue(); // if requests were provided, the queue will immediately try to dequeue itself ...
  }

  isEmpty() {
    return this.requests.length === 0;
  }

  /**
   * Returns if the queue is locked.
   * The queue is locked when:
   *  - it is busy (in the process off being dequeued)
   *  - it is empty (nothing to process)
   *  - there was an error on the queue
   */
  isLocked() {
    if (this.busy || this.isEmpty() || this.error) {
      if (this.error) {
        console.log(`Queue <${this.id}> will not be processed anymore due to a reoccurring failure on the latest task:`);
        console.error(this.error);
        console.log('To restart the queue, fix the issue with consuming service and restart the delta container.');
      }
      return true;
    }
    return false;
  }

  /**
   * Will add a request to the queue, asynchronously start persisting and de-queueing
   *
   * @param request
   */
  enqueue(request) {
    this.requests.push(request);
    // NOTE we do not want to wait on async tasks and 'lock' the queue
    this.persist();
    this.dequeue();
  }

  peek() {
    return !this.isEmpty() ? this.requests[0] : undefined;
  }

  /**
   * Will start de-queuing the queue until it is "locked".
   */
  async dequeue() {
    if (!this.isLocked()) {
      try {
        this.busy = true;
        const {url, method, headers, body} = this.peek();
        await backOff(url, {method, headers, body});
        this.requests.shift(); // only AFTER a successful fetch we remove the request from the queue.
        this.persist();
        this.busy = false;
      } catch (e) {
        this.busy = false;
        this.error = e; // NOTE setting this error parameter will halt any progress on the queue.
      } finally {
        this.dequeue();
      }
    }
  }

  async persist() {
    const filepath = `${QUEUE_PERSIST_FOLDER}${Buffer.from(this.id).toString('base64')}.json`;
    try {
      await fs.writeJSON(filepath, {
        id: this.id,
        requests: this.requests,
      });
    } catch (e) {
      console.log(`Something went wrong while trying to persist queue <${this.id}> to disk`);
      console.error(e);
    }
  }

}

class RequestDispatcher {

  constructor() {
    this.map = {};
  }

  async init() {
    try {
      const files = await fs.readdir(QUEUE_PERSIST_FOLDER);
      for (let filename of files) {
        const queue = await fs.readJson(`${QUEUE_PERSIST_FOLDER}${filename}`);
        if (queue.requests.length !== 0) {
          console.log(`Found queue with id ${queue.id}, rebuilding ...`);
          this.map[queue.id] = new RequestQueue(queue.id, queue.requests);
        } else {
          // clean up possibly dangling files
          fs.remove(`${QUEUE_PERSIST_FOLDER}${filename}`);
        }
      }
    } catch (e) {
      console.log(`Something went wrong while trying to reload the queues from disk`);
      console.error(e);
    }
    return this;
  }

  send(request) {
    const url = request.url;
    if (!this.map[url]) {
      this.map[url] = new RequestQueue(url);
    }
    this.map[url].enqueue(request);
  }

}

export default RequestDispatcher;
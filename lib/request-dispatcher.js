import { backOff } from './back-off';

class RequestQueue {

  constructor() {
    this.requests = [];
    this.busy = false;
    this.error = null;
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
        const {url} = this.peek();
        console.log(
            `The queue for ${url} has stopped being processed due to the reoccurring failure of the last request.`);
        console.error(this.error);
        console.log('To restart the queue, restart the container.');
      }
      return true;
    }
    return false;
  }

  /**
   * Will add a request to the queue, persist it and asynchronously start de-queueing
   *
   * @param request
   */
  enqueue(request) {
    this.requests.push(request);
    // TODO async persist
    this.dequeue(); // we do not want to wait and 'lock' the queue
  }

  peek() {
    return !this.isEmpty() ? this.requests[0] : undefined;
  }

  /**
   * Will start de-queuing the queue until it is empty.
   */
  async dequeue() {
    if (!this.isLocked()) {
      try {
        this.busy = true;
        const {url, method, headers, body} = this.peek();
        await backOff(url, {method, headers, body});
        this.requests.shift(); // only AFTER a successful fetch we remove the request from the queue.
        // TODO async persist
        this.busy = false;
      } catch (e) {
        this.busy = false;
        this.error = e; // NOTE setting this error halts any progress on the queue.
      } finally {
        this.dequeue();
      }
    }
  }

}

class RequestDispatcher {

  constructor() {
    this.map = {};
  }

  send(request) {
    const url = request.url;
    if (!this.map[url]) {
      this.map[url] = new RequestQueue();
    }
    this.map[url].enqueue(request);
  }

}

export default RequestDispatcher;
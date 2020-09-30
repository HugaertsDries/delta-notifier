import { backOff } from './back-off';

class RequestQueue {

  constructor() {
    this.requests = [];
    this.busy = false;
  }

  isEmpty() {
    return this.requests.length === 0;
  }

  enqueue(request) {
    this.requests.push(request);
    // we allow this to run async as we do not want to wait for request to be send out.
    // TODO persist
    this.traverse();
  }

  peek() {
    return !this.isEmpty() ? this.requests[0] : undefined;
  }

  /**
   * Will traverse and send out the request in the queue.
   * If the queue is 'busy' we assume the queue is already being traversed by another instance.
   */
  async traverse() {
    if (!this.busy) {
      this.busy = true;
      while (!this.isEmpty()) {
        const {url, method, headers, body} = this.peek();
        await backOff(url, {
          method, headers, body,
        });
        this.requests.shift();
      }
      this.busy = false;
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
import fetch from 'node-fetch';

class RequestQueue {

  constructor() {
    this.requests = [];
    this.busy = false;
  }

  isEmpty() {
    return this.requests.length === 0;
  }

  enqueue(requests) {
    this.requests.push(requests);
    // we allow this to run async as we do not want to wait for request to be send out.
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
        await backOffFetch(url, {
          method, headers, body,
        });
        this.requests.shift();
      }
      this.busy = false;
    }
  }

}

// TODO add as env-vars
const INITIAL_WAIT = 100;
const BACKOFF = 2.0;
const MAX_RETRY = 10000;

/**
 * Back-Off retry that wraps around fetch-node.
 */
async function backOffFetch(url, options, wait = INITIAL_WAIT, attempt = 0) {
  await fetch(url, options).then(async (res) => {
    if (!res.ok) {
      console.log(`Failed to send request ${options.method} to ${url}`);
      if (wait < MAX_RETRY) {
        console.log(`Retrying in ${wait}ms, attempt ${attempt + 1} failed`);
        await new Promise(r => setTimeout(r, wait));
        wait = (wait * BACKOFF) + Math.floor(Math.random() * 100); // update retry interval
        await backOffFetch(url, options, wait, ++attempt);
      } else {
        console.log(`Max retry off ${MAX_RETRY}ms was reached, delta message was lost ...`);
      }
    } else {
      console.log(`Successfully send ${options.method} to ${url}`);
    }
  }).catch(error => {
    console.log(`Something unexpected went wrong while trying to send request ${options.method} to ${url}`);
    console.error(error);
  });
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
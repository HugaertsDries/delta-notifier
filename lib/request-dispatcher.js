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
        const request = this.peek();
        await this.send(request);
        this.requests.shift();
      }
      this.busy = false;
    }
  }

  async send() {
    console.log('request is being send out ...');
  }

}
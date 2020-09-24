class RequestQueue {

  constructor() {
    this.requests = [];
  }

  isEmpty(){
    return this.requests.length === 0;
  }

  enqueue(requests) {
    this.requests.push(requests);
  }

  peek() {
    return !this.isEmpty() ? this.requests[0] : undefined;
  }

}
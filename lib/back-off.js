import { REQUEST_BACKOFF_RATE, REQUEST_BACKOFF_INITIAL_RETRY_WAIT, REQUEST_BACKOFF_MAX_RETRY_WAIT } from './environment';
import fetch from 'node-fetch';

/**
 * Back-Off retry helper function that wraps around fetch-node.
 */
export async function backOff(url, options, wait = REQUEST_BACKOFF_INITIAL_RETRY_WAIT, attempt = 0) {
  try {
    const response = await fetch(url, options);
    checkResponseStatus(response); // Will throw an error if the status is >= 200 < 300
    console.log(`Successfully send ${options.method} to ${url}`);
  } catch (e) {
    console.log(`Failed to send request ${options.method} to ${url}`);
    console.error(e);
    // The user gives a maximum amount of time there can be waited, if we go over,
    // we stop trying and assume the endpoint is broken
    if (wait < REQUEST_BACKOFF_MAX_RETRY_WAIT) {
      console.log(`Retrying in ${wait}ms, attempt ${attempt + 1} failed`);
      await sleep(wait);
      wait = (wait * REQUEST_BACKOFF_RATE) + Math.floor(Math.random() * 100); // update retry interval
      await backOff(url, options, wait, ++attempt);
    } else {
      console.log(`Max retry off ${REQUEST_BACKOFF_MAX_RETRY_WAIT}ms was reached ...`);
      throw Error(e)
    }
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function checkResponseStatus(response) {
  if (response.ok) { // response.status >= 200 && response.status < 300
    return response;
  } else {
    throw Error(`Status ${response.status}`);
  }
}
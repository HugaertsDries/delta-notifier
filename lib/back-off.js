import { REQUEST_BACKOFF_RATE, REQUEST_BACKOFF_INITIAL_RETRY_WAIT, REQUEST_BACKOFF_MAX_RETRY_WAIT } from './environment';
import fetch from 'node-fetch';
import moment from 'moment';

/**
 * Back-Off retry helper function that wraps around fetch-node.
 */
export async function backOff(url, options, attempt = 0) {
  try {
    const response = await fetch(url, options);
    checkResponseStatus(response); // Will throw an error if the status is >= 200 < 300
    console.log(`Successfully send ${options.method} to ${url}`);
  } catch (e) {
    console.log(`Failed to send request ${options.method} to ${url}`);
    console.error(e);
    const timeout = Math.round((REQUEST_BACKOFF_INITIAL_RETRY_WAIT * Math.pow(1 + REQUEST_BACKOFF_RATE, attempt)));
    // The user gives a maximum amount of time allowed to wait for an endpoint.
    // If we reach this time limit, we stop trying and assume the endpoint is dead or broken.
    if (timeout < REQUEST_BACKOFF_MAX_RETRY_WAIT) {
      ++attempt
      console.log(`Retrying in ${moment.duration(timeout).humanize()}, attempt ${attempt} failed`);
      console.log(`${timeout}ms`)
      await sleep(timeout);
      await backOff(url, options, attempt);
    } else {
      console.log(`Max retry off ${moment.duration(REQUEST_BACKOFF_MAX_RETRY_WAIT).humanize()} was reached ...`);
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
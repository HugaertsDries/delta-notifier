import { app, uuid } from 'mu';
import services from '/config/rules.js';
import bodyParser from 'body-parser';
import dns from 'dns';
import RequestDispatcher from './lib/request-dispatcher';
import {
  DEBUG_DELTA_MATCH, DEBUG_DELTA_SEND,
  DEBUG_TRIPLE_MATCHES_SPEC,
  LOG_REQUESTS,
  LOG_SERVER_CONFIGURATION,
} from './lib/environment';

// Also parse application/json as json
app.use( bodyParser.json( {
  type: function(req) {
    return /^application\/json/.test( req.get('content-type') );
  },
  limit: '500mb'
} ) );

let dispatcher;
new RequestDispatcher().init().then(disp => {
  dispatcher = disp;
});

// Log server config if requested
if( LOG_SERVER_CONFIGURATION )
  console.log(JSON.stringify( services ));

app.get( '/', function( req, res ) {
  res.status(200);
  res.send("Hello, delta notification is running");
} );

app.post( '/', function( req, res ) {
  if( LOG_REQUESTS ) {
    console.log("Logging request body");
    console.log(req.body);
  }

  const changeSets = req.body.changeSets;

  const originalMuCallIdTrail = JSON.parse( req.get('mu-call-id-trail') || "[]" );
  const originalMuCallId = req.get('mu-call-id');
  const muCallIdTrail = JSON.stringify( [...originalMuCallIdTrail, originalMuCallId] );

  changeSets.forEach( (change) => {
    change.insert = change.insert || [];
    change.delete = change.delete || [];
  } );

  // inform watchers
    informWatchers( changeSets, res, muCallIdTrail );

  // push relevant data to interested actors
  res.status(204).send();
} );

async function informWatchers( changeSets, res, muCallIdTrail ){
  services.map( async (entry) => {
    // for each entity
    if( DEBUG_DELTA_MATCH )
      console.log(`Checking if we want to send to ${entry.callback.url}`);

    const matchSpec = entry.match;

    const originFilteredChangeSets = await filterMatchesForOrigin( changeSets, entry );
    if( DEBUG_TRIPLE_MATCHES_SPEC && entry.options.ignoreFromSelf )
      console.log(`There are ${originFilteredChangeSets.length} changes sets not from ${hostnameForEntry( entry )}`);

    let allInserts = [];
    let allDeletes = [];

    originFilteredChangeSets.forEach( (change) => {
      allInserts = [...allInserts, ...change.insert];
      allDeletes = [...allDeletes, ...change.delete];
    } );

    const changedTriples = [...allInserts, ...allDeletes];

    const someTripleMatchedSpec =
        changedTriples
        .some( (triple) => tripleMatchesSpec( triple, matchSpec ) );

    if( DEBUG_TRIPLE_MATCHES_SPEC )
      console.log(`Triple matches spec? ${someTripleMatchedSpec}`);

    if( someTripleMatchedSpec ) {
      // inform matching entities
      if( DEBUG_DELTA_SEND )
        console.log(`Going to send ${entry.callback.method} to ${entry.callback.url}`);

      const request = buildRequest( entry, originFilteredChangeSets, muCallIdTrail )

      if( entry.options && entry.options.gracePeriod ) {
        setTimeout(
          () => dispatcher.send(request),
          entry.options.gracePeriod );
      } else {
        dispatcher.send(request);
      }
    }
  } );
}

function tripleMatchesSpec( triple, matchSpec ) {
  // form of triple is {s, p, o}, same as matchSpec
  if( DEBUG_TRIPLE_MATCHES_SPEC )
    console.log(`Does ${JSON.stringify(triple)} match ${JSON.stringify(matchSpec)}?`);

  for( let key in matchSpec ){
    // key is one of s, p, o
    const subMatchSpec = matchSpec[key];
    const subMatchValue = triple[key];

    if( subMatchSpec && !subMatchValue )
      return false;

    for( let subKey in subMatchSpec )
      // we're now matching something like {type: "url", value: "http..."}
      if( subMatchSpec[subKey] !== subMatchValue[subKey] )
        return false;
  }
  return true; // no false matches found, let's send a response
}


function formatChangesetBody( changeSets, options ) {
  if( options.resourceFormat == "v0.0.1" ) {
    return JSON.stringify(
      changeSets.map( (change) => {
        return {
          inserts: change.insert,
          deletes: change.delete
        };
      } ) );
  }
  if( options.resourceFormat == "v0.0.0-genesis" ) {
    // [{delta: {inserts, deletes}]
    const newOptions = Object.assign({}, options, { resourceFormat: "v0.0.1" });
    const newFormat = JSON.parse( formatChangesetBody( changeSets, newOptions ) );
    return JSON.stringify({
      // graph: Not available
      delta: {
        inserts: newFormat
          .flatMap( ({inserts}) => inserts)
          .map( ({subject,predicate,object}) =>
                ( { s: subject.value, p: predicate.value, o: object.value } ) ),
        deletes: newFormat
          .flatMap( ({deletes}) => deletes)
          .map( ({subject,predicate,object}) =>
                ( { s: subject.value, p: predicate.value, o: object.value } ) )
      }
    });
  } else {
    throw `Unknown resource format ${options.resourceFormat}`;
  }
}

function buildRequest( entry, changeSets, muCallIdTrail ) {
  let request = {
    method: entry.callback.method,
    url: entry.callback.url,
    headers: {
      'Content-Type': 'application/json',
      'MU-AUTH-ALLOWED-GROUPS': changeSets[0].allowedGroups,
      'mu-call-id-trail': muCallIdTrail,
      'mu-call-id': uuid(),
    },
  };

  if (entry.options && entry.options.resourceFormat) {

    // TODO: we now assume the mu-auth-allowed-groups will be the same
    // for each changeSet.  that's a simplification and we should not
    // depend on it.
    request['body'] = formatChangesetBody( changeSets, entry.options );
  }
  return request;
}

async function filterMatchesForOrigin( changeSets, entry ) {
  if( ! entry.options || !entry.options.ignoreFromSelf ) {
    return changeSets;
  } else {
    const originIpAddress = await getServiceIp( entry );
    return changeSets.filter( (changeSet) => changeSet.origin != originIpAddress );
  }
}

function hostnameForEntry( entry ) {
  return (new URL(entry.callback.url)).hostname;
}

async function getServiceIp(entry) {
  const hostName = hostnameForEntry( entry );
  return new Promise( (resolve, reject) => {
    dns.lookup( hostName, { family: 4 }, ( err, address) => {
      if( err )
        reject( err );
      else
        resolve( address );
    } );
  } );
};

const assert = require('assert');
const vm = require('vm');
const fs = require('fs');
let timer, cleared=false, observed;
const context = {
  URL, AbortController, console,
  window:{location:{origin:'https://example.test'},setTimeout(fn){timer=fn;return 1;},clearTimeout(){cleared=true;}},
  fetch:async (url,opts)=>{observed=opts.signal;return {status:200};},
};
vm.createContext(context);
vm.runInContext(fs.readFileSync('static/api.js','utf8'),context);
(async()=>{
  await context.secureFetch('/api/products');
  assert(cleared);
  const caller=new AbortController();
  caller.abort();
  await context.secureFetch('/api/products',{signal:caller.signal});
  assert(observed.aborted);
  context.fetch=async (_url,opts)=>new Promise((_,reject)=>opts.signal.addEventListener('abort',()=>reject(new Error('aborted'))));
  const pending=context.secureFetch('/api/products');
  timer();
  await assert.rejects(pending,/aborted/);
  console.log('API deadlines and cancellation passed');
})().catch(e=>{console.error(e);process.exitCode=1;});

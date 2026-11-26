const fs = require('fs');
const vm = require('vm');
const results = {innerHTML: ''};
const input = {value: '063848900022'};
let finishOld;
const context = {
  console, SEARCH_STOPWORDS:new Set(), allProductsCache:[],
  document:{getElementById(id){return {searchResults:results,searchInput:input,searchField:{value:''}}[id] || null;}},
  window:{clearTimeout(){},setTimeout(){return 1;}},
  apiGetProductByBarcode:()=>new Promise(resolve=>{finishOld=resolve;}),
  apiSearchProducts:async()=>[{id:2,name:'NEW QUERY RESULT'}],
  apiSearchReference:async()=>[],
  looksLikeCompleteRetailBarcode:q=>/^\d{12}$/.test(q),
  esc:String,
};
vm.createContext(context);
vm.runInContext(fs.readFileSync('static/search.js','utf8'),context);
Object.assign(context, {
  cancelReferenceImagePolling(){},cancelSearchImagePolling(){},startSearchImagePolling(){},
  productsByBarcodeFromCache:()=>[],searchProductsFromCache:()=>[],
  groupAndRenderSearchResults:products=>products.map(p=>p.name).join('|'),
  productCard:p=>p.name,
});
(async()=>{
 const old=context.doSearchValue(input.value);
 input.value='new query';
 await context.doSearchValue(input.value);
 const before=results.innerHTML;
 finishOld({id:1,name:'OLD BARCODE RESULT'});
 await old;
 const after=results.innerHTML;
 require('assert').strictEqual(after, before, 'an old barcode response must not replace a newer search');
 console.log('barcode search ordering passed');
})().catch(e=>{console.error(e);process.exitCode=1;});

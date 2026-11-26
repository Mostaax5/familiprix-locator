// Browser -> new search API -> normalized read model -> disposable SQLite.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const {spawn,execFileSync} = require('node:child_process');
const {chromium} = require('playwright');

(async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(),'locator-catalogue-'));
  const probe = net.createServer(); await new Promise(r => probe.listen(0,'127.0.0.1',r));
  const port = probe.address().port; await new Promise(r => probe.close(r));
  const origin = `http://127.0.0.1:${port}`;
  const env = {...process.env,DATABASE_URL:'',DB_PATH:path.join(dir,'test.sqlite'),APP_START_SERVICES:'0',
    STORE_ID:'catalogue-test',STORE_NAME:'Pharmacie de test',APP_EXTERNAL_URL:origin,AUTH_COOKIE_SECURE:'0',AI_ALLOW_PAID:'0',TEST_PORT:String(port)};
  const python = process.env.TEST_PYTHON || 'python';
  execFileSync(python,['-c',`
from database import init_db,connect_db
init_db(); db=connect_db()
rows=[(1,'ADVIL IBUP 200MG CO24','062107004053','1'),(2,'ADVIL IBUP 200MG CO24','062107004053','3'),(3,'Advil 400 mg 24 capsules','000123','1'),(4,'Dentifrice menthe 100ML','000124','2')]
rows += [(i, 'Shampoing doux '+str(i)+' 250ML', 'test'+str(i), '2') for i in range(5,36)]
for id,name,barcode,aisle in rows:
 db.execute("INSERT INTO products(id,name,barcode,aisle,side,section,shelf,position,description,image_url) VALUES(?,?,?,?,'Gauche','1','2',?,'Incorrect generated toothbrush description','https://example.com/wrong.jpg')",(id,name,barcode,aisle,str(id)))
db.commit();db.close()
`],{env});
  const server = spawn(python,['-c','import os; from app import app; app.run(host="127.0.0.1",port=int(os.environ["TEST_PORT"]),debug=False)'],{env,stdio:['ignore','pipe','pipe']});
  let logs=''; server.stdout.on('data',d=>logs+=d);server.stderr.on('data',d=>logs+=d);
  let browser;
  try {
    let ready=false;
    for(let n=0;n<80;n++){try{if((await fetch(origin+'/livez')).ok){ready=true;break;}}catch{}await new Promise(r=>setTimeout(r,100));}
    assert(ready,logs);
    browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox']});
    const context=await browser.newContext({viewport:{width:390,height:844}});
    const page=await context.newPage();const errors=[]; const requests=[];
    page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>requests.push(r.url()));
    await context.route('**/*',r=>r.request().url().startsWith(origin+'/')?r.continue():r.abort());
    await page.goto(origin);
    await page.waitForFunction(()=>document.getElementById('results').getAttribute('aria-busy')==='false');
    assert.equal(await page.locator('.product-card').count(),24);
    assert.match(await page.locator('#catalogue-count').innerText(),/34 produits.*35 emplacements/);
    assert(!requests.some(r=>r.includes('/api/products')||r.includes('wrong.jpg')||r.includes('/vendor/')),'Finder should not download entire legacy catalogue, uncertain photos, or scanner libraries');
    await page.locator('#more').click();await page.waitForFunction(()=>document.querySelectorAll('.product-card').length===34);
    await page.locator('#query').fill('Advil');await page.waitForFunction(()=>document.querySelectorAll('.product-card').length===2);
    await page.locator('.compare-label input').nth(0).check();await page.locator('.compare-label input').nth(1).check();
    await page.locator('#compare-open').click();
    assert.match(await page.locator('#comparison-content').innerText(),/200 mg/);
    assert.match(await page.locator('#comparison-content').innerText(),/400 mg/);
    await page.getByRole('button',{name:'Fermer la comparaison'}).click();
    await page.locator('#query').fill('062107004053');await page.waitForFunction(()=>document.querySelectorAll('.product-card').length===1);
    await page.locator('.product-open').click();
    await page.locator('#detail-content').getByText('Où le trouver · 2 emplacements').waitFor();
    assert.equal(await page.locator('#detail-content .location-row').count(),2);
    await page.getByRole('button',{name:'Fermer la fiche'}).click();
    await page.locator('#query').fill('toothpaste');await page.waitForFunction(()=>document.querySelector('.product-title')?.textContent.includes('Dentifrice'));
    assert.equal(await page.locator('.product-card').count(),1);
    await page.locator('#query').fill('00123456789');await page.locator('#empty').waitFor({state:'visible'});
    await page.locator('#reset').click();await page.waitForFunction(()=>document.querySelectorAll('.product-card').length===24);
    await page.locator('#category').selectOption('hair_shampoo');await page.waitForFunction(()=>document.getElementById('result-count').textContent==='31 produits');
    assert.equal(new URL(page.url()).searchParams.get('category'),'hair_shampoo');
    await page.reload();await page.waitForFunction(()=>document.getElementById('result-count').textContent==='31 produits');
    assert.equal(await page.locator('#category').inputValue(),'hair_shampoo');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Mobile layout overflows horizontally');
    await page.locator('#reset').click();await page.waitForFunction(()=>document.querySelectorAll('.product-card').length===24);
    await page.screenshot({path:process.env.E2E_SCREENSHOT || path.join(dir,'mobile.png'),fullPage:true});
    await page.setViewportSize({width:1440,height:1000});
    await page.screenshot({path:process.env.E2E_DESKTOP_SCREENSHOT || path.join(dir,'desktop.png'),fullPage:true});
    // A direct DB edit, without timestamps, must invalidate the read model.
    execFileSync(python,['-c',`from database import connect_db; db=connect_db(); db.execute("UPDATE products SET name='Dentifrice fraise 100ML' WHERE id=4"); db.commit(); db.close()`],{env});
    await page.locator('#query').fill('dentifrice fraise');await page.waitForFunction(()=>document.querySelector('.product-title')?.textContent.includes('fraise'));
    assert.equal(errors.length,0,JSON.stringify(errors));
    console.log(JSON.stringify({passed:true,viewport:'390x844 and 1440x1000',flows:['pagination','UPC','duplicate placements','bilingual search','comparison','empty state','facets','URL restoration','cross-connection invalidation','no uncertain media','no bulk catalogue download'],pageErrors:errors}));
  } finally {if(browser)await browser.close();server.kill('SIGTERM');}
})().catch(error=>{console.error(error);process.exitCode=1;});

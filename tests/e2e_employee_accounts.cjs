// Real browser -> Flask -> disposable SQLite -> rendered UI. Never uses production data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const {spawn, execFileSync} = require('node:child_process');
const {chromium} = require('playwright');

(async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'locator-accounts-'));
  const probe = net.createServer();
  await new Promise(resolve => probe.listen(0, '127.0.0.1', resolve));
  const port = probe.address().port;
  await new Promise(resolve => probe.close(resolve));
  const origin = `http://127.0.0.1:${port}`;
  const password = 'Synthetic browser fixture password 2026!';
  const env = {...process.env, DATABASE_URL:'', DB_PATH:path.join(directory,'test.db'),
    APP_START_SERVICES:'0', STORE_ID:'browser-test', STORE_NAME:'Magasin de test', STORE_ADDRESS:'',
    APP_EXTERNAL_URL:origin, AUTH_COOKIE_SECURE:'0', AI_ALLOW_PAID:'0',
    TEST_PASSWORD:password, TEST_PORT:String(port)};
  const python = process.env.TEST_PYTHON || 'python';
  execFileSync(python, ['-c', `
import os
from database import init_db, connect_db
from employee_accounts import save_account
init_db()
db=connect_db()
save_account(db, {'login':'owner','display_name':'Test Owner','role':'owner','password':os.environ['TEST_PASSWORD']}, bootstrap=True)
db.commit()
db.close()
`], {env});
  const server = spawn(python, ['-c', 'import os; from app import app; app.run(host="127.0.0.1",port=int(os.environ["TEST_PORT"]),debug=False)'], {env, stdio:['ignore','pipe','pipe']});
  let logs = '';
  server.stdout.on('data', c => logs += c);
  server.stderr.on('data', c => logs += c);
  let browser;
  try {
    let ready = false;
    for (let i=0;i<80;i++) {
      try { if ((await fetch(origin+'/livez')).ok) { ready=true; break; } } catch {}
      await new Promise(resolve => setTimeout(resolve,100));
    }
    assert(ready, 'Flask did not start: '+logs);
    browser = await chromium.launch({headless:true, ...(process.env.CHROMIUM_PATH ? {executablePath:process.env.CHROMIUM_PATH}:{}), args:['--no-sandbox']});
    const context = await browser.newContext({viewport:{width:390,height:844}});
    const page = await context.newPage();
    const pageErrors=[];
    page.on('pageerror', e => pageErrors.push(e.message));
    await context.route('**/*', route => {
      if (route.request().url().startsWith(origin+'/')) return route.continue();
      return route.abort();
    });
    await page.goto(origin);
    await page.waitForFunction(() => typeof _authState !== 'undefined' && _authState.accountMode);
    assert.equal(await page.locator('#storeLabel').innerText(), 'Magasin de test');
    await page.evaluate(() => localStorage.setItem('familiprixStore', JSON.stringify({id:'other-store',name:'Other store'})));
    await page.reload();
    await page.waitForFunction(() => _authState.accountMode);
    assert.equal(await page.locator('#storeLabel').innerText(), 'Magasin de test');
    await page.locator('#tabBtn-add').click();
    await page.locator('#employeeLogin').fill('owner');
    await page.locator('#lockPasswordInput').fill(password);
    await page.locator('#lockLoginButton').click();
    await page.locator('#employeeManageButton').waitFor({state:'visible'});
    await page.locator('#employeeManageButton').click();
    await page.locator('#employeeAccountLogin').fill('store-employee');
    await page.locator('#employeeDisplayName').fill('Employé de test');
    await page.locator('#employeeAccountPassword').fill(password);
    await page.locator('#employeeSaveButton').click();
    await page.locator('#employeeList').getByText('Employé de test', {exact:false}).waitFor();
    assert.equal(await page.locator('#employeeFormError').innerText(), '');
    await page.screenshot({path:process.env.E2E_SCREENSHOT || path.join(directory,'accounts-mobile.png'),fullPage:true});
    const employeeState = await context.storageState();
    assert(!JSON.stringify(employeeState).includes(password), 'Password entered browser storage');
    await page.locator('#employeeManagerDialog').getByRole('button',{name:'Fermer',exact:true}).click();
    await page.locator('#lockButton').click();
    await page.waitForFunction(() => !_authState.authenticated);
    await page.locator('#tabBtn-add').click();
    await page.locator('#employeeLogin').fill('store-employee');
    await page.locator('#lockPasswordInput').fill(password);
    await page.locator('#lockLoginButton').click();
    await page.waitForFunction(() => _authState.role === 'employee');
    assert(await page.locator('#employeeManageButton').isHidden());
    assert.equal(await page.evaluate(async () => (await fetch('/api/admin/employees')).status),403);
    await page.locator('#employeePasswordButton').click();
    await page.locator('#employeeCurrentPassword').fill(password);
    await page.locator('#employeeNewPassword').fill(password+' renewed');
    await page.locator('#employeePasswordSave').click();
    await page.locator('#employeePasswordDialog').waitFor({state:'hidden'});
    await page.reload();
    await page.waitForFunction(() => _authState.role === 'employee');
    assert.equal(pageErrors.length,0,JSON.stringify(pageErrors));
    const stored = JSON.parse(execFileSync(python,['-c',`import json; from database import connect_db; db=connect_db(); print(json.dumps([dict(x) for x in db.execute('SELECT login,role FROM employee_accounts ORDER BY login').fetchall()])); db.close()`],{env,encoding:'utf8'}));
    assert.deepEqual(stored,[{login:'owner',role:'owner'},{login:'store-employee',role:'employee'}]);
    console.log(JSON.stringify({result:'passed',checks:['server-bound store label','owner login','create employee','employee role restriction','password change','session reload','database persistence','mobile layout','no password in storage','no browser runtime errors']},null,2));
  } catch(error) {
    console.error(logs.slice(-6000));
    throw error;
  } finally {
    if(browser) await browser.close();
    server.kill('SIGTERM');
    fs.rmSync(directory,{recursive:true,force:true});
  }
})().catch(error=>{console.error(error);process.exitCode=1;});

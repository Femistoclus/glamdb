"""Opt-in native browser refresh consistency; credentials remain in process memory."""

import os
import re
import subprocess

import pytest

from . import test_real_identity as identity
from .test_real_identity import native_identity


def test_native_concurrent_refresh_and_stale_cookie_keep_session(
    native_identity, monkeypatch
):
    monkeypatch.setattr(identity, "BROWSER_SCRIPT", REFRESH_SCRIPT)
    try:
        result = native_identity.browser("refresh")
    finally:
        native_identity.admin.user_logout(native_identity.organizations[0]["user_id"])
    assert result["initial_status"] == 200, result.get("diagnostics")
    assert result["wait_seconds"] > 30
    assert result["parallel_statuses"] == [200, 200, 200], result.get("diagnostics")
    assert result["stale_status"] == 200, result.get("diagnostics")
    assert result["cleared_session_cookies"] == 0
    assert result["empty_storage"] is True


def test_native_ephemeral_session_store_restart_requires_new_sign_in(
    native_identity, monkeypatch
):
    connection = os.environ.get("GLAMDB_IDENTITY_SESSION_STORE_CONNECTION")
    container = os.environ.get("GLAMDB_IDENTITY_SESSION_STORE_CONTAINER")
    if not connection or not container:
        pytest.skip(
            "requires an explicit disposable session-store connection/container"
        )
    assert re.fullmatch(r"[a-zA-Z0-9_.-]{1,96}", connection)
    assert re.fullmatch(r"glamdb-[a-zA-Z0-9_.-]{1,96}", container)
    monkeypatch.setattr(identity, "BROWSER_SCRIPT", REFRESH_SCRIPT)
    try:
        result = native_identity.browser("session_restart")
    finally:
        subprocess.run(
            ["podman", "--connection", connection, "start", container],
            capture_output=True,
            check=True,
            timeout=30,
        )
        native_identity.admin.user_logout(native_identity.organizations[0]["user_id"])
    assert result["initial_status"] == 200, result.get("diagnostics")
    assert result["old_ticket_status"] == 401, result.get("diagnostics")
    assert result["new_session_status"] == 200, result.get("diagnostics")
    assert result["new_ticket"] is True
    assert result["empty_storage"] is True


REFRESH_SCRIPT = r"""
import fs from 'node:fs';
import https from 'node:https';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const fixture=JSON.parse(fs.readFileSync(input.fixture,'utf8'));
const { chromium }=await import(pathToFileURL(input.root+'/ui/node_modules/@playwright/test/index.mjs').href);
const ca=fs.readFileSync(path.join(path.dirname(input.fixture),'tls.crt'));
let browser,stage='launch',cookie='';const diagnostics=[];
const safeCodes=new Set(['UNAVAILABLE','UNAUTHENTICATED','PERMISSION_DENIED','NOT_FOUND','INVALID_ARGUMENT','CONFLICT','RESOURCE_EXHAUSTED']);
function request(route){
 return new Promise((resolve,reject)=>{
  const req=https.request(input.base+route,{ca,method:'GET',headers:{Cookie:cookie,Accept:'application/json'},timeout:10000},res=>{
   const parts=[];let size=0;
   res.on('data',part=>{size+=part.length;if(size<=16384)parts.push(part);});
   res.on('end',()=>{
    if(res.statusCode>=400){
     const sample={stage,status:res.statusCode,code:null};
     try{if(size<=16384){const body=JSON.parse(Buffer.concat(parts).toString('utf8'));if(safeCodes.has(body?.error?.code))sample.code=body.error.code;}}catch{}
     diagnostics.push(sample);if(diagnostics.length>8)diagnostics.shift();
    }
    resolve({status:res.statusCode,cleared:(res.headers['set-cookie']||[]).filter(value=>/^__Host-glamdb-session(?:_[0-9]+)?=;/.test(value)).length});
   });
  });
  req.on('error',reject);req.on('timeout',()=>req.destroy());req.end();
 });
}
try{
 browser=await chromium.launch({headless:true,executablePath:input.executable||undefined,args:['--ignore-certificate-errors-spki-list='+input.pin]});
 const context=await browser.newContext({ignoreHTTPSErrors:false});
 const page=await context.newPage(),organization=fixture.organizations[0];
 stage='login';await page.goto(input.base+'/');
 await page.locator('input[name=username]').fill(organization.login);
 if(!(await page.locator('input[name=password]').isVisible())){await page.locator('#kc-login').click();await page.locator('input[name=password]').waitFor();}
 await page.locator('input[name=password]').fill(organization.password);await page.locator('#kc-login').click();
 await page.waitForURL(url=>url.origin===new URL(input.base).origin&&url.pathname==='/',{timeout:30000});
 await page.locator('#database-selector').waitFor({timeout:20000});
 const emptyStorage=await page.evaluate(()=>localStorage.length===0&&sessionStorage.length===0);
 // Snapshot only in memory; close the page so its config poll cannot refresh first.
 cookie=(await context.cookies(input.base)).map(value=>value.name+'='+value.value).join('; ');
 const initial=await request('/workbench/session');await page.close();
 let result;
 if(input.mode==='session_restart'){
  stage='store_restart';execFileSync('podman',['--connection',process.env.GLAMDB_IDENTITY_SESSION_STORE_CONNECTION,
   'restart',process.env.GLAMDB_IDENTITY_SESSION_STORE_CONTAINER],{timeout:30000,stdio:'ignore'});
  stage='old_ticket';const old=await request('/workbench/session'),originalCookie=cookie;
  stage='new_sign_in';const fresh=await context.newPage();await fresh.goto(input.base+'/');
  await fresh.locator('#database-selector').waitFor({timeout:20000});
  cookie=(await context.cookies(input.base)).map(value=>value.name+'='+value.value).join('; ');
  const current=await request('/workbench/session');
  result={status:'pass',initial_status:initial.status,old_ticket_status:old.status,
   new_session_status:current.status,new_ticket:cookie!==originalCookie,
   empty_storage:await fresh.evaluate(()=>localStorage.length===0&&sessionStorage.length===0)};
 }else{
 stage='wait_refresh';await new Promise(resolve=>setTimeout(resolve,31000));
 stage='parallel_refresh';const parallel=await Promise.all([request('/workbench/session'),request('/workbench/session'),request('/workbench/session')]);
 // This models an already-issued request arriving after another request refreshed.
 stage='stale_cookie';const stale=await request('/workbench/session');
 result={status:'pass',initial_status:initial.status,wait_seconds:31,
  parallel_statuses:parallel.map(value=>value.status),stale_status:stale.status,
  cleared_session_cookies:parallel.reduce((sum,value)=>sum+value.cleared,0)+stale.cleared,empty_storage:emptyStorage};
 }
 await request('/oauth2/sign_out?rd=%2F');result.diagnostics=diagnostics;console.log(JSON.stringify(result));
}catch(error){console.log(JSON.stringify({status:'fail',stage,error_class:error.name,diagnostics}));process.exitCode=1;}
finally{cookie='';if(browser)await browser.close();}
"""

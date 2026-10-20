// Local JetStream, real short ACK deadlines. No external delivery endpoints.
const assert=require('node:assert/strict'),test=require('node:test');
const {spawn}=require('node:child_process'),net=require('node:net');
const fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {connect}=require('nats');
const {StreamSubscription}=require(process.env.W3A_LEASE_BASELINE_ROOT
  ? path.join(process.env.W3A_LEASE_BASELINE_ROOT,'dist/stream') : '../dist/stream');
const {createTelemetry}=require('../dist');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(read,label){const end=Date.now()+10000;while(Date.now()<end){if(await read())return;await sleep(10);}throw Error(label);}
async function fixture(callback){
  const socket=net.createServer();await new Promise(r=>socket.listen(0,'127.0.0.1',r));
  const port=socket.address().port;await new Promise(r=>socket.close(r));
  const directory=await fs.mkdtemp(path.join(os.tmpdir(),'w3a-sdk-lease-'));
  const server=spawn('nats-server',['-js','-a','127.0.0.1','-p',String(port),'-sd',directory],{stdio:['ignore','ignore','pipe']});
  const exited=new Promise(r=>server.once('exit',r));let logs='';server.stderr.on('data',b=>logs+=b);
  await until(()=>logs.includes('Server is ready'),'NATS startup');
  const nc=await connect({servers:`nats://127.0.0.1:${port}`}),js=nc.jetstream(),jsm=await nc.jetstreamManager();
  await jsm.streams.add({name:'lease',subjects:['lease'],storage:'file'});
  const errors=[],core={js,jsm:{consumers:{
    add:(stream,config)=>jsm.consumers.add(stream,{...config,ack_wait:200_000_000}),
    delete:(...args)=>jsm.consumers.delete(...args),
  }},decode:data=>JSON.parse(Buffer.from(data).toString()),nothrow:fn=>fn(),warn:e=>errors.push(e),uncaughtException:e=>errors.push(e)};
  const sub=new StreamSubscription({telemetry:createTelemetry({logLevel:'error'}).telemetry,core,
    name:'reader',ref:{stream:'lease',subject:'lease'},callback,options:{concurrency:1,batchSize:10}});
  await sub.init();
  return {sub,errors,info:()=>jsm.consumers.info('lease','reader'),publish:n=>js.publish('lease',Buffer.from(JSON.stringify(n))),
    close:async()=>{try{await sub.suspend();await nc.close();}finally{server.kill('SIGTERM');await exited;await fs.rm(directory,{recursive:true});}}};
}

test('queued and active messages renew their leases without early ACK or duplicate callbacks',{timeout:15000},async()=>{
  const received=[];let release;const held=new Promise(r=>release=r);
  const f=await fixture(async({data})=>{received.push(data);if(data===0)await held;await sleep(250);});
  try{
    for(let i=0;i<5;i++)await f.publish(i);
    await until(()=>f.sub._queue.length()===4,'queued fixture');
    await sleep(650); // > 3 original ACK deadlines, including queued items
    const pending=await f.info();
    assert.equal(pending.num_ack_pending,5);assert.equal(pending.ack_floor.stream_seq,0);
    assert.equal(pending.num_redelivered,0);assert.deepEqual(received,[0]);
    release();await until(()=>f.sub._queue.idle(),'all callbacks complete');
    await until(async()=>!(await f.info()).num_ack_pending,'final ACKs');
    assert.deepEqual(received,[0,1,2,3,4]);
    assert.equal(f.sub._pendingMessages.size,0);assert.equal(f.sub._pendingLeaseTimer,undefined);
    assert.deepEqual(f.errors,[]);
  }finally{release();await f.close();}
});

test('suspend keeps leases while draining accepted work, then leaves no renewal timer',{timeout:15000},async()=>{
  let release;const held=new Promise(r=>release=r);let entered=false;
  const f=await fixture(async()=>{entered=true;await held;});
  try{
    await f.publish(1);await until(()=>entered,'callback entered');
    let closed=false;const closing=f.sub.suspend().then(()=>closed=true);
    await sleep(650);
    assert.equal(closed,false);assert.equal((await f.info()).num_ack_pending,1);
    assert.ok(f.sub._pendingLeaseTimer);
    release();await closing;
    assert.equal((await f.info()).num_ack_pending,0);assert.equal(f.sub._pendingLeaseTimer,undefined);
  }finally{release();await f.close();}
});

test('a failed callback stops renewing so requested redelivery still occurs',{timeout:15000},async()=>{
  let attempts=0;
  const f=await fixture(async()=>{if(++attempts===1)throw Object.assign(Error('retry'),{redeliveryDelayMs:350});});
  try{
    await f.publish(1);await until(()=>attempts===2,'NAK redelivery');
    await until(async()=>!(await f.info()).num_ack_pending,'retry ACK');
    assert.equal(f.sub._pendingMessages.size,0);assert.equal(f.sub._pendingLeaseTimer,undefined);
    assert.deepEqual(f.errors,[]);
  }finally{await f.close();}
});

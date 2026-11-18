const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const crypto=require('crypto');
const app=express();app.use(express.static('public'));
const server=http.createServer(app);const io=new Server(server,{pingTimeout:20000});
const PORT=process.env.PORT||3000;
const rooms=new Map();const games=['tap','memory','sort','balance','sequence','signal'];
const rand=n=>crypto.randomInt(n);
const code=()=>{let c;do{c=crypto.randomBytes(3).toString('hex').toUpperCase()}while(rooms.has(c));return c};
const safeName=n=>String(n||'').trim().slice(0,16)||'ななし';
const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
function publicState(r){return {code:r.code,host:r.host,phase:r.phase,players:[...r.players.values()].map(p=>({id:p.id,name:p.name,ready:p.ready,online:!!p.sid,color:p.color})),settings:r.settings,round:r.round,playlist:r.playlist,game:r.current?.id||null,endsAt:r.current?.endsAt||null,progress:r.current?.progress||0,goal:r.current?.goal||0,score:r.score,target:r.target,history:r.history,latest:r.latest||null,wolfId:r.phase==='finished'?[...r.players.values()].find(p=>p.role==='wolf')?.id:null,gameData:r.current?.publicData||null};}
function emit(r){io.to(r.code).emit('state',publicState(r));}
function privateRole(r,p){if(p.sid)io.to(p.sid).emit('role',{role:p.role});}
function cleanup(r){if(r.timer)clearTimeout(r.timer);r.timer=null;}
function drop(s){const c=s.data.room,pid=s.data.pid,r=rooms.get(c);if(!r)return;const p=r.players.get(pid);if(p&&p.sid===s.id){p.sid=null;p.lastSeen=Date.now();emit(r);}s.leave(c);}
function newRound(r){cleanup(r);if(r.round>=r.settings.rounds){r.phase='finished';r.current=null;emit(r);return;}let id=r.playlist[r.round];if(r.settings.mode==='choose'&&!id){r.phase='select';r.current=null;emit(r);return;}if(!id){const pool=games.filter(g=>r.settings.repeat||!r.playlist.includes(g));id=pool[rand(pool.length||games.length)]||games[rand(games.length)];r.playlist[r.round]=id;}
const duration={easy:55000,normal:45000,hard:35000}[r.settings.difficulty];const count=r.players.size;
const goal={tap:count*10,memory:count*3,sort:count*5,balance:count*7,sequence:count*4,signal:count*5}[id];
const publicData={};if(id==='sequence')publicData.pattern=Array.from({length:4},()=>rand(4));if(id==='sort')publicData.target=rand(4);if(id==='signal')publicData.target=rand(3);
const current={id,endsAt:Date.now()+duration,progress:0,goal,publicData,actions:new Map(),lastActions:new Map(),total:0};r.current=current;r.phase='playing';emit(r);
r.timer=setTimeout(()=>finishRound(r),duration+200);
}
function finishRound(r){if(r.phase!=='playing')return;cleanup(r);const c=r.current;const earned=Math.round(clamp(c.progress/c.goal,0,1)*100);r.score+=earned;r.latest={id:c.id,earned,progress:c.progress,goal:c.goal};r.history.push(r.latest);r.phase='results';r.current=null;emit(r);}
function start(r){r.score=0;r.round=0;r.history=[];r.latest=null;r.target=r.settings.rounds*65;r.playlist=Array(r.settings.rounds).fill(null);if(r.settings.mode==='random'){const shuffled=[...games].sort(()=>Math.random()-.5);for(let i=0;i<r.settings.rounds;i++)r.playlist[i]=r.settings.repeat?games[rand(games.length)]:shuffled[i%shuffled.length];}
const ps=[...r.players.values()];const wolf=ps[rand(ps.length)].id;ps.forEach(p=>{p.role=p.id===wolf?'wolf':'crew';p.ready=false;privateRole(r,p);});newRound(r);}
function allowedAction(r,p,a){const c=r.current;if(!c||!p||!p.sid)return false;const now=Date.now(),last=c.lastActions.get(p.id)||0;if(now-last<220)return false;c.lastActions.set(p.id,now);
const action=String(a?.type||'');const val=Number(a?.value);
if(c.id==='tap')return action==='tap';
if(c.id==='memory')return action==='memory'&&Number.isInteger(val)&&val>=0&&val<4;
if(c.id==='sort')return action==='sort'&&Number.isInteger(val)&&val>=0&&val<4;
if(c.id==='balance')return action==='balance'&&(val===-1||val===1);
if(c.id==='sequence')return action==='sequence'&&Number.isInteger(val)&&val>=0&&val<4;
if(c.id==='signal')return action==='signal'&&Number.isInteger(val)&&val>=0&&val<3;
return false;}
function act(r,p,a){if(!allowedAction(r,p,a))return;const c=r.current;const val=Number(a?.value);let delta=0;
if(c.id==='tap')delta=1;
if(c.id==='memory')delta=val===((c.total+1)%4)?1:-1;
if(c.id==='sort')delta=val===c.publicData.target?1:-1;
if(c.id==='balance')delta=val===(c.total%2===0?1:-1)?1:-1;
if(c.id==='sequence')delta=val===c.publicData.pattern[c.total%4]?1:-1;
if(c.id==='signal')delta=val===c.publicData.target?1:-1;
c.total++;c.progress=clamp(c.progress+delta,0,c.goal);if(c.id==='sort')c.publicData.target=rand(4);if(c.id==='signal')c.publicData.target=rand(3);
emit(r);if(c.progress>=c.goal)finishRound(r);
}
io.on('connection',s=>{
s.on('create',(d={},cb=()=>{})=>{drop(s);const r={code:code(),host:null,players:new Map(),phase:'lobby',settings:{rounds:7,mode:'random',difficulty:'normal',repeat:true},round:0,playlist:[],score:0,target:455,history:[],current:null,timer:null};const pid=crypto.randomUUID();r.host=pid;const p={id:pid,sid:s.id,name:safeName(d.name),ready:false,color:0,role:null};r.players.set(pid,p);rooms.set(r.code,r);s.join(r.code);s.data.room=r.code;s.data.pid=pid;cb({ok:true,code:r.code,pid});emit(r);});
s.on('join',(d={},cb=()=>{})=>{const r=rooms.get(String(d.code||'').trim().toUpperCase());if(!r)return cb({ok:false,error:'部屋が見つかりません'});let p=d.pid&&r.players.get(d.pid);if(!p){if(r.phase!=='lobby')return cb({ok:false,error:'試合中の部屋には新規参加できません'});if(r.players.size>=6)return cb({ok:false,error:'満員です'});p={id:crypto.randomUUID(),sid:null,name:safeName(d.name),ready:false,color:r.players.size%6,role:null};r.players.set(p.id,p);}drop(s);if(p.sid&&p.sid!==s.id){const old=io.sockets.sockets.get(p.sid);if(old){old.leave(r.code);old.data.room=null;}}p.sid=s.id;p.lastSeen=null;s.join(r.code);s.data.room=r.code;s.data.pid=p.id;cb({ok:true,code:r.code,pid:p.id});emit(r);if(p.role)privateRole(r,p);});
s.on('ready',(v,cb=()=>{})=>{const r=rooms.get(s.data.room),p=r?.players.get(s.data.pid);if(!r||!p||r.phase!=='lobby')return cb({ok:false});p.ready=!!v;emit(r);cb({ok:true});});
s.on('settings',(v={},cb=()=>{})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||r.phase!=='lobby')return cb({ok:false});const mode=['random','choose'].includes(v.mode)?v.mode:'random';const difficulty=['easy','normal','hard'].includes(v.difficulty)?v.difficulty:'normal';r.settings={rounds:clamp(v.rounds,3,10),mode,difficulty,repeat:!!v.repeat};r.players.forEach(p=>p.ready=false);emit(r);cb({ok:true});});
s.on('transfer',(pid,cb=()=>{})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||!r.players.has(pid))return cb({ok:false});r.host=pid;emit(r);cb({ok:true});});
s.on('start',(cb=()=>{})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||r.phase!=='lobby'||r.players.size<3||r.players.size>6||![...r.players.values()].every(p=>p.ready&&p.sid))return cb({ok:false,error:'3〜6人全員の準備と接続が必要です'});start(r);cb({ok:true});});
s.on('choose',(id,cb=()=>{})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||r.phase!=='select'||!games.includes(id))return cb({ok:false});if(!r.settings.repeat&&r.playlist.includes(id)&&r.playlist.some(x=>x===null)&&r.playlist.filter(Boolean).length<games.length)return cb({ok:false,error:'同じゲームは選べません'});r.playlist[r.round]=id;newRound(r);cb({ok:true});});
s.on('action',a=>{const r=rooms.get(s.data.room),p=r?.players.get(s.data.pid);if(r&&r.phase==='playing')act(r,p,a);});
s.on('next',()=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||r.phase!=='results')return;r.round++;newRound(r);});
s.on('rematch',()=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.data.pid||r.phase!=='finished')return;r.phase='lobby';r.players.forEach(p=>{p.role=null;p.ready=false});r.history=[];r.score=0;r.round=0;r.latest=null;emit(r);});
s.on('leave',()=>{const r=rooms.get(s.data.room);if(!r)return;const pid=s.data.pid;drop(s);r.players.delete(pid);if(r.players.size===0){cleanup(r);rooms.delete(r.code);}else{if(r.host===pid)r.host=r.players.keys().next().value;emit(r);}s.data.room=null;s.data.pid=null;});
s.on('disconnect',()=>drop(s));
});
setInterval(()=>{for(const r of rooms.values()){for(const [id,p] of r.players){if(!p.sid&&p.lastSeen&&Date.now()-p.lastSeen>30*60*1000&&r.phase==='lobby')r.players.delete(id);}if(!r.players.size){cleanup(r);rooms.delete(r.code);}else if(!r.players.get(r.host)?.sid){const candidate=[...r.players.values()].find(p=>p.sid);if(candidate&&r.phase==='lobby'){r.host=candidate.id;emit(r);}}}},60000).unref();
server.listen(PORT,()=>console.log('Ready on '+PORT));

/**
 * testbots.js — 40 auto-playing test bots for Bingo
 *
 * What it does:
 *   1. REGISTER : inserts 40 Ethiopian-named users straight into the DB
 *                 (the real flow needs a Telegram contact share, which a script can't do)
 *   2. DEPOSIT  : each bot sends a real `depositRequest` over WebSocket, then this script
 *                 approves all of them through your admin REST API
 *   3. PLAY     : each bot joins a room, picks a random free card, auto-marks every called
 *                 number, auto-claims BINGO when it wins, and keeps playing until it can no
 *                 longer afford the stake
 *
 * Setup:
 *   npm install ws pg
 *
 * Run (use a TEST deployment + TEST database, NOT your live one):
 *   DATABASE_URL="postgres://..."  \
 *   SERVER_URL="https://your-test-service.onrender.com" \
 *   ADMIN_TOKEN="251XXXXXXXXX" \
 *   node testbots.js
 *
 * Optional env vars:
 *   BOTS=40          number of bots
 *   DEPOSIT=10000    ETB deposited per bot
 *   STAKE=st10       st10 or st20
 *   CARDS=1          cards per bot per game (1 or 2; 2 drains balances twice as fast)
 *
 * Cleanup (removes all test users and their rows):
 *   node testbots.js --cleanup
 */

const WebSocket = require('ws');
const { Pool } = require('pg');

// ─── CONFIG ──────────────────────────────────────────────────
const DATABASE_URL = process.env.DATABASE_URL;
const SERVER_URL   = (process.env.SERVER_URL || '').replace(/\/+$/, '');
const ADMIN_TOKEN  = process.env.ADMIN_TOKEN;
const BOTS         = parseInt(process.env.BOTS || '40', 10);
const DEPOSIT      = parseInt(process.env.DEPOSIT || '10000', 10);
const STAKE_ID     = process.env.STAKE || 'st10';
const CARDS        = Math.min(2, Math.max(1, parseInt(process.env.CARDS || '1', 10)));
const STAKE_AMOUNT = STAKE_ID === 'st20' ? 20 : 10;
const TOTAL_CARDS  = 400;

// Reserved ID/phone ranges so test users never collide with real players
const TID_BASE   = 7000000000;
const PHONE_BASE = 251770000000;

const NAMES = [
  'Abebe Kebede', 'Almaz Tesfaye', 'Bekele Girma', 'Birtukan Alemu', 'Dawit Haile',
  'Eden Mulugeta', 'Fikru Tadesse', 'Genet Assefa', 'Habtamu Desta', 'Hana Worku',
  'Kidus Bekele', 'Lemlem Yilma', 'Mekdes Abera', 'Meron Getachew', 'Nahom Tefera',
  'Rahel Demissie', 'Samson Negash', 'Selam Berhane', 'Tigist Hailu', 'Yohannes Teka',
  'Zelalem Ayele', 'Aster Bogale', 'Biniam Lemma', 'Chaltu Gemechu', 'Dagim Wolde',
  'Elsa Mengistu', 'Firaol Tolera', 'Gelila Kassa', 'Henok Zewdu', 'Ruth Shiferaw',
  'Kaleb Fekadu', 'Liya Solomon', 'Mikias Amare', 'Netsanet Eshetu', 'Robel Tsegaye',
  'Sara Wondimu', 'Tewodros Admasu', 'Wegene Dinku', 'Yared Belay', 'Zeritu Hussen',
  'Amanuel Tilahun', 'Bethlehem Gebre', 'Cherinet Abate', 'Dereje Feleke', 'Feven Asrat',
  'Mulu Yimer', 'Tsion Gashaw', 'Yonas Bayou', 'Helen Gizaw', 'Eyob Mekonnen'
];

// ─── HELPERS ─────────────────────────────────────────────────
// On Render a worker that exits is restarted automatically, which would re-register
// the bots and reset their balances. So on Render we idle instead of exiting.
function exitOrIdle(code) {
  if (process.env.RENDER) {
    log('⏸️ Finished. Idling so Render does not restart the run. Suspend or delete this worker.');
    setInterval(() => {}, 1 << 30);
  } else {
    process.exit(code);
  }
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rand  = (a, b) => a + Math.floor(Math.random() * (b - a + 1));
const log   = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

function wsUrl() {
  return SERVER_URL.replace(/^http/, 'ws');
}

// Same card generator and win check as server.js
function seededRandom(seed) {
  let s = seed;
  return () => { s |= 0; s = s + 0x6D2B79F5 | 0; let t = Math.imul(s ^ s >>> 15, 1 | s); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; };
}
function checkWin(nums, called, marked) {
  const cs = new Set(called), ms = new Set(marked || []); ms.add(12);
  const hit = i => i === 12 || (cs.has(nums[i]) && ms.has(i));
  return [[0,1,2,3,4],[5,6,7,8,9],[10,11,12,13,14],[15,16,17,18,19],[20,21,22,23,24],
          [0,5,10,15,20],[1,6,11,16,21],[2,7,12,17,22],[3,8,13,18,23],[4,9,14,19,24],
          [0,6,12,18,24],[4,8,12,16,20],[0,4,20,24]].some(p => p.every(i => hit(i)));
}
function markedFor(nums, called) {
  const cs = new Set(called), out = [];
  nums.forEach((n, i) => { if (i === 12 || cs.has(n)) out.push(i); });
  return out;
}

function makePool() {
  return new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
}

// ─── CLEANUP ─────────────────────────────────────────────────
async function cleanup() {
  const pool = makePool();
  const lo = TID_BASE + 1, hi = TID_BASE + 1000;
  const ids = (await pool.query(
    `SELECT id FROM users WHERE telegram_id::bigint BETWEEN $1 AND $2`, [lo, hi])).rows.map(r => r.id);
  if (!ids.length) { log('No test users found.'); await pool.end(); return; }
  for (const t of ['transactions', 'deposit_requests', 'withdrawal_requests', 'game_participants']) {
    try { await pool.query(`DELETE FROM ${t} WHERE user_id = ANY($1)`, [ids]); }
    catch (e) { log(`(skipped ${t}: ${e.message})`); }
  }
  await pool.query(`DELETE FROM users WHERE id = ANY($1)`, [ids]);
  log(`Removed ${ids.length} test users.`);
  await pool.end();
}

// ─── STEP 1: REGISTER ────────────────────────────────────────
async function registerUsers() {
  const pool = makePool();
  for (let i = 0; i < BOTS; i++) {
    const tid = String(TID_BASE + i + 1);
    const phone = String(PHONE_BASE + i + 1);
    const name = NAMES[i % NAMES.length].slice(0, 20);
    await pool.query(
      `INSERT INTO users(telegram_id,name,phone,balance) VALUES($1,$2,$3,0)
       ON CONFLICT(telegram_id) DO UPDATE SET name=$2, phone=$3, balance=0`,
      [tid, name, phone]);
  }
  await pool.end();
  log(`✅ Registered ${BOTS} Ethiopian-named test users in the database`);
}

// ─── BOT ─────────────────────────────────────────────────────
const bots = [];
let gamesSeen = 0;
let stopping = false;

class Bot {
  constructor(i) {
    this.i = i;
    this.tid = String(TID_BASE + i + 1);
    this.name = NAMES[i % NAMES.length].slice(0, 20);
    this.balance = 0;
    this.ws = null;
    this.done = false;
    this.reset();
    this.taken = new Set();
    this.depositOk = false;
    this.wins = 0; this.games = 0;
  }
  reset() {
    this.selecting = false; this.pendingPick = false;
    this.cards = [];        // [{id, nums, slot}]
    this.claimed = false;
    this.called = [];
  }
  send(o) { if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(o)); }

  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(wsUrl());
      this.ws.on('open', () => this.send({ type: 'telegramAuth', telegramId: this.tid }));
      this.ws.on('message', raw => {
        let m; try { m = JSON.parse(raw); } catch { return; }
        this.onMsg(m, resolve);
      });
      this.ws.on('error', e => reject(e));
      this.ws.on('close', () => { if (!this.done && !stopping) log(`⚠️ ${this.name} disconnected`); });
    });
  }

  onMsg(m, onAuth) {
    switch (m.type) {
      case 'authSuccess':
        if (!m.isRegistered) { log(`❌ ${this.name} not found in DB`); }
        this.balance = m.balance;
        if (onAuth) onAuth();
        break;
      case 'balanceUpdate': this.balance = m.balance; break;
      case 'depositSubmitted': this.depositOk = true; break;
      case 'error':
        if (/already taken/i.test(m.message)) { this.pendingPick = false; this.pick(); }
        else if (/Need .* ETB/i.test(m.message)) { this.pendingPick = false; this.outOfMoney(); }
        break;

      case 'joinedRoom':
        this.taken = new Set();
        if (m.status === 'playing') { this.selecting = false; }   // joined mid-game: spectate
        else { this.selecting = true; }
        break;
      case 'cardPoolUpdate':
        this.taken = new Set(m.pool.filter(c => c.taken && !c.takenByMe).map(c => c.id));
        this.pick();
        break;
      case 'cardPoolDiff':
        m.changes.forEach(c => { if (c.takenByMe) return; c.taken ? this.taken.add(c.id) : this.taken.delete(c.id); });
        break;
      case 'cardSelected':
        this.pendingPick = false;
        this.cards = this.cards.filter(c => c.slot !== m.slot);
        this.cards.push({ id: m.cardId, nums: m.cardNumbers, slot: m.slot });
        this.pick();
        break;

      case 'yourCard':
        this.selecting = false; this.pendingPick = false; this.claimed = false; this.called = [];
        this.cards = [];
        if (m.cardId) this.cards.push({ id: m.cardId, nums: m.cardNumbers, slot: 1 });
        if (m.cardId2) this.cards.push({ id: m.cardId2, nums: m.cardNumbers2, slot: 2 });
        this.games++;
        break;
      case 'numberCalled':
        this.called = m.calledNumbers;
        this.autoClaim();
        break;
      case 'gameOver':
        gamesSeen++;
        if (m.winners && m.winners.includes(this.name)) this.wins++;
        break;
      case 'disqualified': log(`🚫 ${this.name} disqualified (should not happen)`); break;
      case 'backToCardSelection':
        this.balance = m.balance;
        this.reset(); this.taken = new Set();
        if (this.balance < STAKE_AMOUNT) { this.outOfMoney(); return; }
        this.selecting = true;
        break;
    }
  }

  // Pick random free cards until we hold CARDS of them
  pick() {
    if (this.done || !this.selecting || this.pendingPick) return;
    if (this.cards.length >= CARDS) return;
    if (this.balance < STAKE_AMOUNT * (this.cards.length + 1)) {
      if (this.cards.length === 0) this.outOfMoney();
      return;
    }
    const mine = new Set(this.cards.map(c => c.id));
    let id, tries = 0;
    do { id = rand(1, TOTAL_CARDS); tries++; } while ((this.taken.has(id) || mine.has(id)) && tries < 200);
    this.pendingPick = true;
    this.send({ type: 'selectCard', cardId: id, slot: this.cards.length + 1 });
  }

  // "Auto mode": mark every called number, claim BINGO as soon as a line is complete
  autoClaim() {
    if (this.claimed || !this.cards.length) return;
    const c1 = this.cards.find(c => c.slot === 1), c2 = this.cards.find(c => c.slot === 2);
    const m1 = c1 ? markedFor(c1.nums, this.called) : [];
    const m2 = c2 ? markedFor(c2.nums, this.called) : [];
    const win = (c1 && checkWin(c1.nums, this.called, m1)) || (c2 && checkWin(c2.nums, this.called, m2));
    if (!win) return;
    this.claimed = true;
    setTimeout(() => {
      this.send({ type: 'claimBingo', markedIndices: m1, cardId2: c2 ? c2.id : null, markedIndices2: m2 });
    }, rand(200, 1200));   // small human-like delay (claim window is ~4.8s)
  }

  outOfMoney() {
    if (this.done) return;
    this.done = true;
    this.send({ type: 'leaveRoom' });
    log(`💸 ${this.name} is out of money (balance ${this.balance} ETB, ${this.games} games, ${this.wins} wins)`);
    setTimeout(() => { try { this.ws.close(); } catch {} }, 500);
    checkAllDone();
  }

  join() { this.send({ type: 'joinRoom', stakeId: STAKE_ID }); }
}

function activeBots() { return bots.filter(b => !b.done); }

function checkAllDone() {
  const left = activeBots().length;
  if (left === 0) return finish('All bots are out of money.');
  if (left < 2) return finish('Fewer than 2 bots left, so no more games can start.');
}

function summary() {
  const total = bots.reduce((s, b) => s + b.balance, 0);
  log(`📊 games finished: ${gamesSeen} | active bots: ${activeBots().length}/${bots.length} | total bot balance: ${total.toFixed(0)} ETB`);
}

function finish(reason) {
  if (stopping) return;
  stopping = true;
  log(`🏁 ${reason}`);
  summary();
  console.log('\nName                 Balance   Games  Wins');
  bots.forEach(b => console.log(
    `${b.name.padEnd(20)} ${String(b.balance).padStart(7)}   ${String(b.games).padStart(5)}  ${String(b.wins).padStart(4)}`));
  bots.forEach(b => { try { b.ws.close(); } catch {} });
  setTimeout(() => exitOrIdle(0), 1000);
}

// ─── STEP 2: DEPOSIT ─────────────────────────────────────────
async function depositAll() {
  const stamp = Date.now();
  for (const b of bots) b.send({ type: 'depositRequest', amount: DEPOSIT, txRef: `TESTBOT-${b.tid}-${stamp}` });
  await sleep(3000);
  const sent = bots.filter(b => b.depositOk).length;
  log(`📨 ${sent}/${bots.length} deposit requests submitted`);

  const headers = { 'x-admin-token': ADMIN_TOKEN, 'Content-Type': 'application/json' };
  const res = await fetch(`${SERVER_URL}/api/admin/deposits?status=pending`, { headers });
  if (!res.ok) throw new Error(`Could not list deposits (HTTP ${res.status}). Check ADMIN_TOKEN.`);
  const pending = (await res.json()).filter(d => String(d.tx_ref || '').startsWith('TESTBOT-'));
  log(`🔎 found ${pending.length} pending test deposits to approve`);
  for (const d of pending) {
    await fetch(`${SERVER_URL}/api/admin/deposits/${d.id}/approve`, { method: 'POST', headers });
    await sleep(50);
  }
  await sleep(2000);
  const funded = bots.filter(b => b.balance >= DEPOSIT).length;
  log(`💰 ${funded}/${bots.length} bots funded with ${DEPOSIT} ETB`);
  if (funded < bots.length) log('⚠️ Some bots are not funded. The admin list only shows 50 pending deposits at a time; clear other pending deposits and rerun.');
}

// ─── MAIN ────────────────────────────────────────────────────
(async () => {
  if (process.argv.includes('--cleanup')) {
    if (!DATABASE_URL) { console.error('DATABASE_URL is required'); process.exit(1); }
    return cleanup();
  }
  if (!DATABASE_URL || !SERVER_URL || !ADMIN_TOKEN) {
    console.error('Missing env vars. Required: DATABASE_URL, SERVER_URL, ADMIN_TOKEN');
    process.exit(1);
  }
  if (BOTS > 900) { console.error('BOTS must be 900 or fewer'); process.exit(1); }

  // Tiny status page so this can run as a Render Web Service (free plan needs an open port,
  // and an uptime monitor pinging this URL keeps it awake).
  if (process.env.PORT) {
    require('http').createServer((req, res) => {
      const total = bots.reduce((s, b) => s + b.balance, 0);
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end(`ok | active bots ${activeBots().length}/${bots.length} | games finished ${gamesSeen} | total balance ${total.toFixed(0)} ETB\n`);
    }).listen(process.env.PORT);
  }

  log(`Starting ${BOTS} bots | deposit ${DEPOSIT} ETB | stake ${STAKE_AMOUNT} ETB | ${CARDS} card(s)/game`);
  await registerUsers();

  for (let i = 0; i < BOTS; i++) bots.push(new Bot(i));
  for (const b of bots) { await b.connect(); await sleep(80); }
  log(`🔌 ${bots.length} bots connected`);

  await depositAll();

  // Join the room with a small stagger
  for (const b of bots) { setTimeout(() => b.join(), rand(0, 5000)); }
  log('🎮 Bots are joining and playing in auto mode...');

  setInterval(summary, 30000);
  process.on('SIGINT', () => finish('Stopped by you (Ctrl+C).'));
})().catch(e => {
  console.error('Fatal:', e.message);
  bots.forEach(b => { try { b.ws.close(); } catch {} });
  setTimeout(() => exitOrIdle(1), 500);
});

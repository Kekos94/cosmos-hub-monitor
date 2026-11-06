const test = require('node:test');
const assert = require('node:assert/strict');
const monitor = require('../web/dashboard.js');

test('missing amounts stay distinct from a measured zero', () => {
  for (const value of ['',null,undefined,'NaN','Infinity']) assert.equal(monitor.number(value),null);
  assert.equal(monitor.number('0'),0);
  assert.equal(monitor.number('0.0513269'),0.0513269);
});
test('period uses inclusive UTC days without including a future row', () => {
  const rows=['2026-10-29','2026-10-30','2026-11-05','2026-11-06'].map(date=>({date}));
  assert.deepEqual(monitor.filter(rows,'7','2026-11-05').map(r=>r.date),['2026-10-30','2026-11-05']);
});
test('independent criteria retain independent latest dates', () => {
  assert.equal(monitor.latest([{date:'2026-11-04'}]).date,'2026-11-04');
  assert.equal(monitor.latest([{date:'2026-11-05'}]).date,'2026-11-05');
  assert.equal(monitor.latest([]),null);
});
test('chart series neither fills missing days nor replaces unknown values with zero', () => {
  assert.deepEqual(monitor.series([{date:'2026-11-05',fees:''},{date:'2026-11-03',fees:'7'}],'fees'),[
    {date:'2026-11-03',value:7},{date:'2026-11-05',value:null}]);
});

test('net amounts distinguish missing values, measured zero and one INJ base unit', () => {
  assert.equal(monitor.amount('', 'en-GB'),null);
  assert.equal(monitor.amount('0', 'en-GB'), '0');
  assert.equal(monitor.amount('0.000000000000000001', 'en-GB'), '1E-18');
  assert.equal(monitor.amount('-0.000000000000000001', 'en-GB'), '-1E-18');
  assert.equal(monitor.amount('6249.251203','en-GB',true),'+6,249.251203');
});

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

test('the weekly net chart keeps seven dates, signs, measured zero and gaps', () => {
  const rows = [
    {date: '2026-11-02', net_amount: '2.25'},
    {date: '2026-11-04', net_amount: '-1'},
    {date: '2026-11-06', net_amount: '0'},
    {date: '2026-11-08', net_amount: ''},
    {date: '2026-11-09', net_amount: '8'},
  ];
  const points = monitor.netWeek(rows, '2026-11-08');
  assert.equal(points[0].date, '2026-11-02');
  assert.equal(points[6].date, '2026-11-08');
  assert.deepEqual(points.map(point => point.value), [2.25, null, -1, null, 0, null, null]);
});

test('a partial calendar week still ends on Sunday', () => {
  const end = monitor.weekSunday('2026-11-09');
  const points = monitor.netWeek([{date: '2026-11-10', net_amount: '-5'}], end);
  assert.equal(end, '2026-11-15');
  assert.equal(points[0].date, '2026-11-09');
  assert.deepEqual(points.map(point => point.value), [null, -5, null, null, null, null, null]);
  assert.equal(monitor.weekSunday('2026-11-08'), '2026-11-08');
});

const { test } = require('tap');
const Fastify = require('fastify');
const arrowPlugin = require('../index');
const { createTable, validateUpload } = require('./util');
const { tableToIPC } = require('apache-arrow');

const POST_TABLE = { url: `/`, method: `POST`, headers: { 'content-type': `application/octet-stream` } };

test(`it should read a table as an octet stream`, async (t) => {

  const expected = createTable();
  const payload = Buffer.from(tableToIPC(expected).buffer);

  await Fastify().register(arrowPlugin)
    .post('/', validateUpload.bind(null, [expected]))
    .inject({ ...POST_TABLE, payload }).then((res) => {
      t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
      t.strictEqual(res.body, 'pass');
    })
    .catch(t.threw);
});

test(`it should read multiple tables as an octet stream`, async (t) => {

  const expected = [createTable(), createTable()];
  const payload = Buffer.concat(expected.map((x) => tableToIPC(x)));

  await Fastify().register(arrowPlugin)
    .post('/', validateUpload.bind(null, expected))
    .inject({ ...POST_TABLE, payload }).then((res) => {
      t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
      t.strictEqual(res.body, 'pass');
    })
    .catch(t.threw);
});

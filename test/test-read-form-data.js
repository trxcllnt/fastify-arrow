const { test } = require('tap');
const Fastify = require('fastify');
const FormData = require('form-data')
const arrowPlugin = require('../index');
const { tableToIPC } = require('apache-arrow');
const { createTable, validateUpload } = require('./util');

const POST_TABLE = { url: `/`, method: `POST` };

test(`it should read a table as multipart/form-data`, async (t) => {

  const expected = createTable();
  const form = new FormData();
  form.append('table', Buffer.from(tableToIPC(expected)));

  await Fastify().register(arrowPlugin)
    .post('/', async (request, reply) => {
      t.ok(request.isMultipart());
      await validateUpload([expected], request, reply);
    })
    .inject({ ...POST_TABLE, payload: form, headers: form.getHeaders() })
    .then((res) => {
      t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
      t.strictEqual(res.body, 'pass');
    })
    .catch(t.threw);
});

test(`it should read multiple tables as multipart/form-data`, async (t) => {

  const expected = [createTable(), createTable()];
  const form = new FormData();
  form.append('table0', Buffer.from(tableToIPC(expected[0])));
  form.append('table1', Buffer.from(tableToIPC(expected[1])));

  await Fastify().register(arrowPlugin)
    .post('/', async (request, reply) => {
      t.ok(request.isMultipart());
      await validateUpload(expected, request, reply);
    })
    .inject({ ...POST_TABLE, payload: form, headers: form.getHeaders() })
    .then((res) => {
      t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
      t.strictEqual(res.body, 'pass');
    })
    .catch(t.threw);
});

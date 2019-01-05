const { test } = require('tap');
const Fastify = require('fastify');
const FormData = require('form-data')
const arrowPlugin = require('../index');
const { AsyncIterable } = require('ix');
const { Table } = require('apache-arrow');
const { createTable, compareTables } = require('./util');

const POST_TABLE = { url: `/`, method: `POST` };

test(`it should read a table as multipart/form-data`, async (t) => {

    const expected = createTable();
    const form = new FormData();
    form.append('table', Buffer.from(expected.serialize()));

    await Fastify().register(arrowPlugin)
        .post('/', (request, reply) => {
            t.ok(request.isMultipart());
            request
                .recordBatches().map(Table.from)
                .map((actual) => compareTables(expected, actual))
                .map((_, index) => index === 0 ? 'pass' : 'fail')
                .catch(AsyncIterable.as.bind(0, 'fail')).takeLast(1)
                .pipe(reply.type('text/plain; charset=utf-8').stream());
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
    form.append('table0', Buffer.from(expected[0].serialize()));
    form.append('table1', Buffer.from(expected[1].serialize()));

    await Fastify().register(arrowPlugin)
        .post('/', (request, reply) => {
            t.ok(request.isMultipart());
            request
                .recordBatches().map(Table.from)
                .map((actual, i) => compareTables(expected[i], actual))
                .map((_, index) => index === 1 ? 'pass' : 'fail')
                .catch(AsyncIterable.as.bind(0, 'fail')).takeLast(1)
                .pipe(reply.type('text/plain; charset=utf-8').stream());
        })
        .inject({ ...POST_TABLE, payload: form, headers: form.getHeaders() })
        .then((res) => {
            t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
            t.strictEqual(res.body, 'pass');
        })
        .catch(t.threw);
});

const { test } = require('tap');
const Fastify = require('fastify');
const arrowPlugin = require('../index');
const { AsyncIterable } = require('ix');
const { Table } = require('apache-arrow');
const { createTable, compareTables } = require('./util');

const POST_TABLE = { url: `/`, method: `POST`, headers: { 'content-type':  `application/octet-stream` } };

test(`it should read a table as an octet stream`, async (t) => {

    const expected = createTable();
    const payload = Buffer.from(expected.serialize());

    await Fastify().register(arrowPlugin)
        .post('/', (request, reply) => {
            request
                .recordBatches().map(Table.from)
                .map((actual) => compareTables(expected, actual))
                .map((_, index) => index === 0 ? 'pass' : 'fail')
                .catch(AsyncIterable.as.bind(0, 'fail')).takeLast(1)
                .pipe(reply.type('text/plain; charset=utf-8').asStream());
        })
        .inject({ ...POST_TABLE, payload }).then((res) => {
            t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
            t.strictEqual(res.body, 'pass');
        })
        .catch(t.threw);
});

test(`it should read multiple tables as an octet stream`, async (t) => {

    const expected = [createTable(), createTable()];
    const payload = Buffer.concat(expected.map((x) => x.serialize()));

    await Fastify().register(arrowPlugin)
        .post('/', (request, reply) => {
            request
                .recordBatches().map(Table.from)
                .map((actual, i) => compareTables(expected[i], actual))
                .map((_, index) => index === 1 ? 'pass' : 'fail')
                .catch(AsyncIterable.as.bind(0, 'fail')).takeLast(1)
                .pipe(reply.type('text/plain; charset=utf-8').asStream());
        })
        .inject({ ...POST_TABLE, payload }).then((res) => {
            t.strictEqual(res.headers['content-type'], 'text/plain; charset=utf-8');
            t.strictEqual(res.body, 'pass');
        })
        .catch(t.threw);
});

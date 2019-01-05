const { test } = require('tap');
const Fastify = require('fastify');
const arrowPlugin = require('../index');
const { AsyncIterable, Iterable } = require('ix');
const { createTable, compareTables } = require('./util');
const {
    Table, RecordBatch, DataType, FloatVector,
    RecordBatchReader, RecordBatchStreamWriter
} = require('apache-arrow');

const getHeaders = { 'accepts':  `application/octet-stream` };
const postHeaders = {
    'accepts':  `application/octet-stream`,
    'content-type':  `application/octet-stream`
};

test('it should reply with a table', async (t) => {

    const expected = createTable();

    await Fastify().register(arrowPlugin)
        .get('/', (req, reply) => {
            RecordBatchStreamWriter
                .writeAll(expected)
                .pipe(reply.stream())
        })
        .inject({ url: `/`, method: `GET`, headers: getHeaders })
        .then((res) => {
            compareTables(expected, Table.from(res.rawPayload));
            t.strictEqual(res.headers['content-type'], 'application/octet-stream');
        })
        .catch(t.threw);
});

test('it should reply with multiple tables', async (t) => {

    const expected = [createTable(), createTable()];

    await Fastify().register(arrowPlugin)
        .get('/', (req, reply) => {
            AsyncIterable
                .from(expected)
                .flatMap(RecordBatchStreamWriter.writeAll)
                .pipe(reply.stream({ objectMode: false }))
        })
        .inject({ url: `/`, method: `GET`, headers: getHeaders })
        .then((res) => {
            for (const reader of RecordBatchReader.readAll(res.rawPayload)) {
                compareTables(expected.shift(), Table.from(reader));
            }
            t.strictEqual(res.headers['content-type'], 'application/octet-stream');
        })
        .catch(t.threw);
});

test(`it should accept a table and respond with a different one`, async (t) => {

    const expectedIn = createTable();
    const expectedOut = averageFloatCols(expectedIn);
    const payload = Buffer.from(expectedIn.serialize());

    await Fastify().register(arrowPlugin)
        .post('/', (request, reply) => {
            request.recordBatches()
                .map(Table.from).map(averageFloatCols)
                .flatMap(RecordBatchStreamWriter.writeAll)
                .pipe(reply.stream({ objectMode: false }));
        })
        .inject({ url: `/`, method: `POST`, headers: postHeaders, payload })
        .then((res) => {
            compareTables(expectedOut, Table.from(res.rawPayload));
            t.strictEqual(res.headers['content-type'], 'application/octet-stream');
        })
        .catch(t.threw);

    function averageFloatCols(table) {
        const fields = table.schema.fields.filter(DataType.isFloat);
        const names = fields.map(({ name }) => `${name}_avg`);
        const averages = fields
            .map((f) => table.getColumn(f.name))
            .map((xs) => Iterable.from(xs).average())
            .map((avg) => FloatVector.from(new Float32Array([avg])))
        return new Table(RecordBatch.from(averages, names));
    }
});

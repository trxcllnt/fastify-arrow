require('ix/Ix.node');
require('ix/add/iterable/from');
require('ix/add/asynciterable/from');

const { test } = require('tap');
const Fastify = require('fastify');
const arrowPlugin = require('../index');
const { createTable, compareTables } = require('./util');
const { AsyncIterable, Iterable } = require('ix/Ix.node');
const {
  Table, DataType,
  RecordBatchReader, RecordBatchStreamWriter,
  tableToIPC, tableFromIPC, tableFromArrays, vectorFromArray
} = require('apache-arrow');

const getHeaders = { 'accepts': `application/octet-stream` };
const postHeaders = { ...getHeaders, 'content-type': `application/octet-stream` };

test('it should reply with a table', async (t) => {

  const expected = createTable();

  await Fastify().register(arrowPlugin)
    .get('/', (req, reply) => {
      RecordBatchStreamWriter.writeAll(expected).pipe(reply.stream());
    })
    .inject({ url: `/`, method: `GET`, headers: getHeaders })
    .then((res) => {
      compareTables(expected, tableFromIPC(res.rawPayload));
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
        .pipe(reply.stream({ objectMode: false }));
    })
    .inject({ url: `/`, method: `GET`, headers: getHeaders })
    .then((res) => {
      for (const reader of RecordBatchReader.readAll(res.rawPayload)) {
        compareTables(expected.shift(), tableFromIPC(reader));
      }
      t.strictEqual(res.headers['content-type'], 'application/octet-stream');
    })
    .catch(t.threw);
});

test(`it should accept a table and respond with a different one`, async (t) => {

  const expectedIn = createTable();
  const expectedOut = averageFloatCols(expectedIn);
  const payload = Buffer.from(tableToIPC(expectedIn).buffer);

  await Fastify().register(arrowPlugin)
    .post('/', (request, reply) => {
      request.recordBatches()
        .map(async (reader) => averageFloatCols(new Table(await reader.readAll())))
        .flatMap((table) => RecordBatchStreamWriter.writeAll(table))
        .pipe(reply.stream({ objectMode: false }));
    })
    .inject({ url: `/`, method: `POST`, headers: postHeaders, payload })
    .then((res) => {
      debugger;
      compareTables(expectedOut, tableFromIPC(res.rawPayload));
      t.strictEqual(res.headers['content-type'], 'application/octet-stream');
    })
    .catch(t.threw);

  /**
   *
   * @param {Table} table
   */
  function averageFloatCols(table) {
    /** @type import('apache-arrow').Field[] */
    const fields = table.schema.fields.filter(DataType.isFloat);
    /** @type string[] */
    const names = fields.map(({ name }) => `${name}_avg`);
    const averages = fields
      .map((f) => table.getChild(f.name))
      .map((xs) => Iterable.from(xs).average())
      .map((avg) => vectorFromArray(new Float32Array([avg])));
    return tableFromArrays(names.reduce((xs, name, i) => ({
      ...xs, [name]: averages[i]
    }), {}));
  }
});

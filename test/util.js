const randomatic = require('randomatic');
const {
  Schema, Field, Table, RecordBatch,
  Utf8, Utf8Vector,
  Float32, FloatVector,
  util: { createElementComparator },
  RecordBatchStreamWriter
} = require('apache-arrow');

module.exports = {
  createTable,
  compareTables,
  validateUpload
};

const str = ((opts) =>
  (length) => randomatic('?', length, opts)
)({ chars: `abcdefghijklmnopqrstuvwxyz0123456789_` });

function createTable() {
  const schema = new Schema([new Field('str', new Utf8()), new Field('num', new Float32())]);
  const batches = [
    new RecordBatch(schema, 3, [
      Utf8Vector.from([str(5), str(5), str(5)]),
      FloatVector.from(Float32Array.from([1, 2, 3]))
    ]),
    new RecordBatch(schema, 3, [
      Utf8Vector.from([str(5), str(5), str(5)]),
      FloatVector.from(Float32Array.from([4, 5, 6]))
    ]),
    new RecordBatch(schema, 3, [
      Utf8Vector.from([str(5), str(5), str(5)]),
      FloatVector.from(Float32Array.from([7, 8, 9]))
    ])
  ];
  return new Table(schema, batches);
}

function compareTables(expected, actual) {
  if (actual.length !== expected.length) {
    throw new Error(`length: ${actual.length} !== ${expected.length}`);
  }
  if (actual.numCols !== expected.numCols) {
    throw new Error(`numCols: ${actual.numCols} !== ${expected.numCols}`);
  }
  (() => {
    const getChildAtFn = expected instanceof Table ? 'getColumnAt' : 'getChildAt';
    for (let i = -1, n = actual.numCols; ++i < n;) {
      const v1 = actual[getChildAtFn](i);
      const v2 = expected[getChildAtFn](i);
      compareVectors(v1, v2);
    }
  })();
}

function compareVectors(actual, expected) {

  if ((actual == null && expected != null) || (expected == null && actual != null)) {
    throw new Error(`${actual == null ? `actual` : `expected`} is null, was expecting ${actual == null ? expected : actual} to be that also`);
  }

  let props = ['type', 'length', 'nullCount'];

  (() => {
    for (let i = -1, n = props.length; ++i < n;) {
      const prop = props[i];
      if (`${actual[prop]}` !== `${expected[prop]}`) {
        throw new Error(`${prop}: ${actual[prop]} !== ${expected[prop]}`);
      }
    }
  })();

  (() => {
    for (let i = -1, n = actual.length; ++i < n;) {
      let x1 = actual.get(i), x2 = expected.get(i);
      if (!createElementComparator(x2)(x1)) {
        throw new Error(`${i}: ${x1} !== ${x2}`);
      }
    }
  })();

  (() => {
    let i = -1, r1, r2, x1, x2;
    const it1 = actual[Symbol.iterator]();
    const it2 = expected[Symbol.iterator]();
    while (!(r1 = it1.next()).done && !(r2 = it2.next()).done) {
      ++i;
      x1 = r1.value;
      x2 = r2.value;
      if (!createElementComparator(x2)(x1)) {
        throw new Error(`${i}: ${x1} !== ${x2}`);
      }
    }
  })();
}

async function validateUpload(expected, request, reply) {
  let i = 0, pass = true;
  for await (const batches of request.recordBatches()) {
    const actual = await Table.from(batches);
    try {
      compareTables(expected[i], actual);
    } catch (err) {
      pass = false;
    }
    if (++i >= expected.length || !pass) { break; }
  }
  reply.type('text/plain; charset=utf-8').send(pass ? 'pass' : 'fail');
}

if (require.main === module) {

  RecordBatchStreamWriter.writeAll(demoData()).pipe(process.stdout);

  function* demoData(batchLen = 10, numBatches = 5) {
    const rand = Math.random.bind(Math);
    const randstr = ((randomatic, opts) =>
      (len) => randomatic('?', len, opts)
    )(require('randomatic'), { chars: `abcdefghijklmnopqrstuvwxyz0123456789_` });

    let schema;
    for (let i = -1; ++i < numBatches;) {
      const str = new Array(batchLen);
      const num = new Float32Array(batchLen);
      (() => {
        for (let i = -1; ++i < batchLen; str[i] = randstr((num[i] = rand() * (2 ** 4)) | 0));
      })();
      const columns = [Utf8Vector.from(str), FloatVector.from(num)];
      schema || (schema = Schema.from(columns, ['strings', 'floats']));
      yield new RecordBatch(schema, batchLen, columns);
    }
  }
}

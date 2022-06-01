const randomatic = require('randomatic');
const {
  Table, Utf8,
  RecordBatchReader,
  RecordBatchStreamWriter,
  tableFromArrays, vectorFromArray,
  util: { createElementComparator },
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
  const makeTable = () => tableFromArrays({
    str: vectorFromArray([
      str(5),
      str(5),
      str(5),
    ], new Utf8),
    num: vectorFromArray(new Float32Array([
      Math.random() * (2 ** 4),
      Math.random() * (2 ** 4),
      Math.random() * (2 ** 4),
    ])),
  });
  return makeTable().concat(makeTable(), makeTable());
}

function compareTables(expected, actual) {
  if (actual.numRows !== expected.numRows) {
    throw new Error(`numRows: ${actual.numRows} !== ${expected.numRows}`);
  }
  if (actual.numCols !== expected.numCols) {
    throw new Error(`numCols: ${actual.numCols} !== ${expected.numCols}`);
  }
  (() => {
    for (let i = -1, n = actual.numCols; ++i < n;) {
      compareVectors(actual.getChildAt(i), expected.getChildAt(i));
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
    const reader = await RecordBatchReader.from(batches);
    const actual = new Table(await reader.readAll());
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
  RecordBatchStreamWriter.writeAll(createTable()).pipe(process.stdout);
}

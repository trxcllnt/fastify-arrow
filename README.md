# fastify-arrow
A Fastify plugin for sending and receiving columnar tables with [Apache Arrow](https://github.com/apache/arrow) as optimized, zero-copy binary streams.

This module decorates the fastify `Request` with a `recordBatches()` method that returns an [IxJS `AsyncIterable`](https://github.com/ReactiveX/IxJS#asynciterable) of Arrow `RecordBatchReaders`.

Each inner `RecordBatchReader` is an `AsyncIterableIterator<RecordBatch>`, leading to the following signature:
```ts
Request.prototype.recordBatches = () => AsyncIterable<AsyncIterable<RecordBatch>>;
```

`AsyncIterable` is native in JS via the `[Symbol.asyncIterator]()` and `for await...of` protocols. You can create an `Ix.AsyncIterable` from a `NodeJS.ReadableStream` with `AsyncIterable.fromNodeStream()`. You can also `pipe()` an `AsyncIterable` to a `NodeJS.WritableStream` to more easily transition between the functional and imperative APIs available in node and Arrow.

Arrow RecordBatches are full-width, length-wise slices of a Table. To illustrate, the following table contains three RecordBatches, and each RecordBatch has three rows:
```
"row_id" |      "utf8: Utf8" |  "floats: Float32"    ___
       0 |          "sh679x" |  6.308125972747803       |
       1 |    "u9joo443zl38" | 12.003445625305176       | <-- RecordBatch 1
       2 |  "4b2f5pcyp_nisb" |  14.00214672088623    ___|
       3 |        "rfmuc50d" |  8.512785911560059       |
       4 |  "1u7ygm51_2cvye" | 14.949934959411621       | <-- RecordBatch 2
       5 |        "xffgrp9x" |  8.687625885009766    ___|
       6 |   "9vhc_g3_lqx4v" | 13.841902732849121       |
       7 | "4bxi6ioh8cssq12" | 15.428414344787598       | <-- RecordBatch 3
       8 |         "zjcxb2s" | 7.1155924797058105    ___|
```

You can generate a table similar to the above by installing the dependencies, then executing the following command from the repository TLD:
```sh
$ node test/util.js | npx arrow2csv
```

This module also decorates fastify's `Reply` with a convenient `stream()` method, returning a pass-through stream hooked up to the http `ServerResponse`.

### Send Arrow RecordBatch streams

```js
const Fastify = require('fastify');
const arrowPlugin = require('fastify-arrow');
const fastify = Fastify().register(require('fastify-arrow'));
const {
    Schema, DataType,
    Table, RecordBatch,
    Utf8Vector, FloatVector,
    RecordBatchStreamWriter,
} = require('apache-arrow');

fastify.get(`/data`, (request, reply) => {
    RecordBatchStreamWriter
        .writeAll(demoData())
        .pipe(reply.stream());
});

(async () => {
    const res = await fastfiy.inject({
        url: '/data', method: `GET`, headers: {
            'accepts': `application/octet-stream`
        }
    });
    console.log(Table.from(res.body)); // Table<{ strings: Utf8, floats: Float32 }>
})();

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
```

### Receive Arrow RecordBatch streams

```js
const { AsyncIterable } = require('ix');
const { createWriteStream } = require('fs');
const eos = require('util').promisify(require('stream').finished);

fastify.post(`/update`, (request, reply) => {
    request.recordBatches()
        .map((recordBatches) => eos(recordBatches
            .pipe(createWriteStream('./new_data.arrow'))))
        .map(() => 'ok').catch(() => AsyncIterable.of('fail'))
        .pipe(reply.type('application/octet-stream').stream());
});

(async () => {
    const res = await fastfiy.inject({
        url: '/data', method: `POST`, headers: {
            'accepts':  `text/plain; charset=utf-8`,
            'content-type':  `application/octet-stream`
        },
        payload: RecordBatchStreamWriter.writeAll(demoData()).toNodeStream()
    });
    console.log(res.body); // 'ok' | 'fail'
})();
```

### Send and receive Arrow RecordBatch streams

```js
fastify.post(`/avg_floats`, (request, reply) => {
    request.recordBatches()
        .map((recordBatches) => averageFloatCols(Table.from(recordBatches)))
        .pipe(RecordBatchStreamWriter.throughNode({ autoDestroy: false }))
        .pipe(reply.type('application/octet-stream').stream());
});

(async () => {
    const writer = RecordBatchStreamWriter.writeAll(demoData());
    const averages = await fastfiy.inject({
        url: '/data', method: `POST`,
        payload: writer.toNodeStream(),
        headers: {
            'accepts':  `application/octet-stream`,
            'content-type':  `application/octet-stream`
        },
    });
    console.log(Table.from(res.body)); // Table<{ floats_avg: Float32 }>
})();

function averageFloatCols(table) {
    const fields = table.schema.fields.filter(DataType.isFloat);
    const names = fields.map(({ name }) => `${name}_avg`);
    const averages = fields
        .map((f) => table.getColumn(f.name))
        .map((xs) => Iterable.from(xs).average())
        .map((avg) => FloatVector.from(new Float32Array([avg])))
    return new Table(RecordBatch.from(averages, names));
}
```

const { Readable, PassThrough } = require('stream');
const { as: asAsyncIterable } = require('ix/asynciterable');
const { AsyncByteQueue, AsyncByteStream, RecordBatchReader } = require('apache-arrow');

const AsyncQueue = Object.getPrototypeOf(AsyncByteQueue);

module.exports = require('fastify-plugin')(fastifyArrowPlugin, {
  fastify: '>= 2.x', name: 'fastify-arrow'
});

function fastifyArrowPlugin(fastify, opts, next) {

  if (!fastify.hasRequestDecorator('multipart')) {
    fastify.register(require('@fastify/multipart'), opts);
  }

  // Add a stub octet-stream parser so fastify doesn't reject payloads with content-type octet-stream
  fastify.addContentTypeParser('octet-stream', opts, (_, next) => { next(); });

  fastify.decorateReply('stream', replyAsStream);
  fastify.decorateRequest('recordBatches', readRecordBatches);

  next();
}

function replyAsStream(xs = { objectMode: false }) {
  const stream = new PassThrough(xs);
  this.send(stream);
  return stream;
}

/**
 * @returns AsyncIterable<RecordBatchReader>
 */
function readRecordBatches() {
  if (this.isMultipart()) {
    return asAsyncIterable(RecordBatchReader.readAll(fromMultipart(this)));
  }
  return asAsyncIterable(RecordBatchReader.readAll(Readable.from(this.raw)));
}

async function* fromMultipart(request) {

  const files = new AsyncQueue();
  const body = request.body || (request.body = {});

  request.multipart(
    (_field, file, _name) => { files.write(file); },
    (err) => { err != null ? files.abort(err) : files.close(); }
  ).on('field', (k, v) => body[k] = v);

  for await (const file of files) {
    yield* new AsyncByteStream(file);
  }
}

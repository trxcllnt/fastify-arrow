const { AsyncIterable } = require('ix');
const { PassThrough } = require('stream');
const { AsyncQueue } = require('apache-arrow/io/interfaces');
const { AsyncByteStream, RecordBatchReader } = require('apache-arrow');

module.exports = require('fastify-plugin')(fastifyArrowPlugin, {
    fastify: '>= 2.x', name: 'fastify-arrow'
});

function fastifyArrowPlugin(fastify, opts, next) {

    if (!fastify.hasRequestDecorator('multipart')) {
        fastify.register(require('fastify-multipart'), opts);
    }

    // Add a stub octet-stream parser so fastify doesn't reject payloads with content-type octet-stream
    fastify.addContentTypeParser('octet-stream', opts, (_, next) => { next(); });

    fastify.decorateReply('asStream', replyAsStream);
    fastify.decorateRequest('recordBatches', readRecordBatches);

    next();
}

function replyAsStream(xs = { objectMode: false }) {
    const stream = new PassThrough(xs);
    return this.send(stream) || stream;
}

/**
 * @returns AsyncIterable<RecordBatchReader>
 */
function readRecordBatches() {
    const source = this.isMultipart() ? fromMultipart(this) : this.raw;
    return AsyncIterable.from(async function* () {
        const reader = await RecordBatchReader.from(source);
        try {
            while (!(await reader.reset().open(false)).closed) {
                yield reader;
            }
        } finally { reader.cancel(); }
    }());
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

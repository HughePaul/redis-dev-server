
const net = require('net');
const fs = require('fs');
const stream = require('stream');
const moment = require('moment');

const CR = 13;
const BUFFER = 36;
const ARRAY = 42;
const SIMPLE = 43;
const ERROR = 45;
const NUMBER = 58;

const string = val => typeof val === 'string' ? val : val.toString ? val.toString() : String(val);
const number = val => typeof val === 'number' ? val : parseInt(val, 10);
const summary = (max = 32) => val => val.length > max ? val.slice(0, max - 1).toString() + '…' : val.toString();
const pattern = pattern => {
    pattern = pattern
        .replace(/([\\.(){}^$])/g, '\\$1')
        .replace(/([*?])/g, '.$1')
        .replace(/\\\\.([*?])/g, '\\$1')
        .replace(/\\\\([[\]])/g, '\\$1');
    pattern = '^' + pattern + '$';
    return new RegExp(pattern);
};

class RedisConnection {
    constructor(server, inStream, outStream, options = {}) {
        this.options = options;
        this.server = server;
        this.data = server.data;
        this.inStream = inStream;
        this.outStream = outStream;
        this.stack = [];
        this.type = null;
        this.name = this.options.name || 'TCP:' + this.inStream.remoteAddress + ':' + this.inStream.remotePort;

        this.log('Connected');

        inStream.on('data', data => {
            this.parse(data);
        });

        inStream.on('close', () => {
            this.log('Disconnected');
            this.destroy();
        });
        inStream.on('end', () => {
            this.log('Ended');
            this.destroy();
        });
        inStream.on('error', err => {
            this.log('Connection error', err);
            this.destroy();
        });
    }

    log(...args) {
        if (!this.options.quiet && this.server) this.server.log(this.name, ...args);
    }

    parse(data) {
        let p = 0;
        while (p < data.length) {

            // choose first char as data type
            if (!this.type) {
                this.type = data[p];
                if (this.type === CR) {
                    p+=2;
                    this.type = null;
                    continue;
                }
                if (![ARRAY, BUFFER, NUMBER, SIMPLE, ERROR].includes(this.type)) {
                    throw new Error('Unknown type: ' + this.type);
                }
                this.line = null;
                this.length = null;
                p++;
                continue;
            }

            // collect buffer content
            if (this.type === Buffer) {
                let bytesToCopy = this.length - (this.line ? this.line.length : 0);
                let bytesLeftToRead = data.length - p;
                bytesToCopy = Math.min(bytesToCopy, bytesLeftToRead);
                this.append(data, p, p + bytesToCopy);
                p += bytesToCopy;
                if (this.line.length === this.length) {
                    p += 2;
                    this.item(BUFFER, this.line);
                    this.type = null;
                }
                continue;
            }

            // collect type data to line end
            let lineEnd = data.indexOf(CR, p);
            if (lineEnd === -1) {
                this.append(data, p);
                p += data.length;
                continue;
            }

            this.append(data, p, lineEnd);
            p = lineEnd + 2;

            if (this.type === BUFFER) {
                this.length = number(this.line);
                if (this.length === -1) {
                    this.item(null, null);
                    this.type = null;
                    continue;
                }

                this.type = Buffer;
                this.line = null;
                continue;
            }

            this.item(this.type, this.line);
            this.type = null;
            continue;
        }
    }

    append(data, start, end) {
        let part = data.slice(start, end);
        this.line = this.line ? Buffer.concat([this.line, part]) : part;
    }

    item(type, line) {
        switch (type) {
        case ARRAY:
            {
                let array = [];
                array.size = number(line);
                this.stack.push(array);
            }
            break;
        case NUMBER:
            this.push(number(line));
            break;
        case SIMPLE:
        case ERROR:
            this.push(string(line));
            break;
        default:
            this.push(line);
        }
    }

    push(item) {
        let array = this.stack[this.stack.length - 1];
        if (!Array.isArray(array)) return this.run(item);
        array.push(item);
        if (array.length === array.size) {
            this.stack.pop();
            this.push(array);
        }
    }

    run(args) {
        if (!Array.isArray(args)) args = [ args ];
        let command = string(args.shift());
        let method = 'run' + command.slice(0, 1).toUpperCase() + command.slice(1).toLowerCase();
        if (!this[method]) return this.error('UNKNOWN COMMAND ' + command);

        this.log(command.toUpperCase(), ...(args.map(summary(50))));
        this[method].apply(this, args);
    }

    respond(line) {
        switch (typeof line) {
        case 'undefined':
        case 'object':
            if (Array.isArray(line)) {
                this.write('*' + line.length + '\r\n');
                line.forEach(item => this.respond(item));
                return;
            }
            if (line instanceof Buffer) {
                this.write('$' + line.length + '\r\n');
                this.write(line).write('\r\n');
                return;
            }
            return this.write('$-1\r\n');
        case 'string': return this.write('+' + line + '\r\n');
        case 'number': return this.write(':' + line + '\r\n');
        }
        throw new Error('Unknown item to write: ' + line);
    }

    write(data) {
        if (this.outStream) this.outStream.write(data);
        return this;
    }

    error(line) {
        this.log('Error:', line);
        this.write('-' + line + '\r\n');
    }

    destroy() {
        this.server = null;
        this.buffer = null;
        this.line = null;
        this.stack = null;
        this.data = null;
        this.inStream = null;
        this.outStream = null;
    }

    runInfo() {
        this.respond('OK');
    }

    runEcho(val) {
        this.respond(val);
    }

    runSave() {
        this.server.save();
    }

    runSelect(index) {
        index = number(index);
        if (index !== 0 ) return this.error('SELECT can only select db index 0');
        this.respond('OK');
    }

    runFlushall() {
        this.data.clear();
        this.data.changed = true;
        this.respond('OK');
    }

    runFlushdb() {
        this.runFlushall();
    }

    runDbsize() {
        this.respond(this.data.length);
    }

    runClient(command, ...args) {
        command = string(command).toUpperCase();
        switch (command) {
        case 'SETNAME':
            this.name = string(args.shift());
            return this.respond('OK');
        case 'GETNAME':
            return this.respond(this.name);
        default:
            this.error('Unsupported CLIENT command: ' + command);
        }
    }

    runPing() {
        this.respond('PONG');
    }

    runExists(key) {
        key = string(key);
        let exists = this.data.has(key);
        if (exists) return this.respond(1);
        this.respond(0);
    }

    runType(key) {
        key = string(key);
        let data = this.data.get(key);
        if (!data) return this.respond('');
        this.respond('string');
    }


    runGet(key) {
        key = string(key);
        let data = this.data.get(key);
        if (!data) return this.respond(null);
        this.respond(data.v);
    }

    runMget(...args) {
        let results = args.map(key => {
            let data = this.data.get(string(key));
            return data ? data.v : null;
        });
        this.respond(results);
    }

    runSetex(key, ttl, value) {
        this.runSet(key, value, 'EX', ttl);
    }

    runPsetex(key, ttl, value) {
        this.runSet(key, value, 'PX', ttl);
    }

    runSet(key, value, ...args) {
        key = string(key);
        let data = { v: value, e: null };
        while (args.length) {
            let arg = string(args.shift()).toUpperCase();
            if (arg === 'EX') data.e = Date.now() + number(args.shift()) * 1000;
            else if (arg === 'PX') data.e = Date.now() + number(args.shift());
            else if (arg === 'NX') if (this.data.has(key)) return this.respond(null);
            else if (arg === 'XX') if (!this.data.has(key)) return this.respond(null);
            else return this.error('Unknown SET argument: ' + arg);
        }
        this.data.set(key, data);
        this.data.changed = true;
        this.respond('OK');
    }

    runMset(...args) {
        while (args.length) {
            let key = string(args.shift());
            let value = args.shift();
            let data = { v: value, e: null };
            this.data.set(key, data);
        }
        this.data.changed = true;
        this.respond('OK');
    }

    runMsetnx(...args) {
        let set = [];
        while (args.length) {
            let key = string(args.shift());
            if (this.data.has(key)) return this.respond(0);
            let value = args.shift();
            set.push({ key, value });
        }
        while (set.length) {
            let {key, value} = set.shift();
            let data = { v: value, e: null };
            this.data.set(key, data);
        }
        this.data.changed = true;
        this.respond(1);
    }

    runDel(...keys) {
        keys = keys.map(key => string(key));
        keys = keys.filter(key => this.data.has(key));
        keys.forEach(key => this.data.delete(key));
        this.data.changed = true;
        this.respond(keys.length);
    }

    runKeys(match) {
        if (!match) return this.error('KEYS requires pattern');
        match = pattern(string(match));
        let keys = Array.from(this.data.keys());
        keys = keys.filter(key => match.test(key));
        this.respond(keys);
    }

    runScan(cursor, ...args) {
        if (!cursor) return this.error('SCAN requires cursor');
        cursor = number(cursor);
        let match = null;
        let count = 10;
        while (args.length) {
            let arg = string(args.shift()).toUpperCase();
            if (arg === 'COUNT') count = number(args.shift());
            else if (arg === 'MATCH') match = pattern(string(args.shift()));
            else return this.error('Unknown SCAN argument: ' + arg);
        }
        let keys = Array.from(this.data.keys());
        if (match) keys = keys.filter(key => match.test(key));
        let len = keys.length;
        let end  = cursor + count;
        keys = keys.slice(cursor, end);
        let next = end <= len ? end : 0;
        this.respond([next, keys]);
    }

    runExpireat(key, stamp) {
        key = string(key);
        stamp = number(stamp);
        let data = this.data.get(key);
        if (!data) this.respond(0);
        data.e = stamp;
        this.data.changed = true;
        this.respond(1);
    }

    runExpire(key, ttl) {
        key = string(key);
        ttl = number(ttl);
        this.log('Expire', key, ttl);
        this.runExpireat(key, Date.now() + ttl * 1000);
    }

    runPexpire(key, ttl) {
        key = string(key);
        ttl = number(ttl);
        this.log('PExpire', key, ttl);
        this.runExpireat(key, Date.now() + ttl);
    }

    runTtl(key) {
        key = string(key);
        let data = this.data.get(key);
        if (!data) return this.respond(-2);
        if (!data.e) return this.respond(-1);
        this.respond(Math.floor((data.e - Date.now()) / 1000));
    }

    runPttl(key) {
        key = string(key);
        let data = this.data.get(key);
        if (!data) return this.respond(-2);
        if (!data.e) return this.respond(-1);
        this.respond(data.e - Date.now());
    }

    runDumpall() {
        let now = Date.now();
        this.respond(['FLUSHDB']);
        this.data.forEach((data, key) => {
            let command = ['SET', key, data.v ];
            if (data.e && data.e > now) {
                command.push('PX');
                command.push(data.e - now);
            }
            this.respond(command);
        });
    }

    runQuit() {
        if (this.inStream && this.inStream.end) this.inStream.end();
        else if (this.inStream && this.inStream.close) this.inStream.close();
        else if (this.inStream && this.inStream.destroy) this.inStream.destroy();

        if (this.outStream && this.outStream.end) this.outStream.end();
        else if (this.outStream && this.outStream.close) this.outStream.close();
        else if (this.outStream && this.outStream.destroy) this.outStream.destroy();
    }
}

class RedisServer {
    constructor({
        address = '127.0.0.1',
        port = 6379,
        filename = require('path').resolve(__dirname, 'persist.db'),
        saveInterval = 10,
        expireInterval = 5,
        logger = console.log
    } = {}) {
        this.logger = logger;

        this.data = new Map();

        this.filename = filename;
        this.load();

        let server = net.createServer(socket => {
            new RedisConnection(this, socket, socket);
        });

        server.listen(port, address, err => {
            if (err) return this.log('SERVER', 'Listen error:', err);
            this.log('SERVER', 'Listening on:', address + ':' + port);
        });

        if (saveInterval) setInterval(() => this.save(filename), saveInterval * 1000);
        if (expireInterval) setInterval(() => this.expire(), expireInterval * 1000);
    }

    load() {
        if (!this.filename) return;
        this.log('SERVER', 'Loading:', this.filename);
        try {
            let inStream = fs.createReadStream(this.filename);
            new RedisConnection(this, inStream, null, { name: 'LOADER', quiet: true });
        } catch (e)  {
            this.log('SERVER', 'Load error:', e);
        }
    }

    save() {
        if (!this.data.changed) return;
        if (!this.filename) return;
        this.data.changed = false;
        this.log('SERVER', 'Saving:', this.filename);
        let inStream = new stream.PassThrough();
        let outStream = fs.createWriteStream(this.filename);
        new RedisConnection(this, inStream, outStream, { name: 'SAVER', quiet: true });
        inStream.write('*1\r\n+DUMPALL\r\n');
        inStream.write('*1\r\n+QUIT\r\n');
        inStream.resume();
    }

    expire() {
        let now = Date.now();
        this.data.forEach((data, key) => {
            if (data.e && data.e < now) {
                this.log('SERVER', 'Expired:', key);
                this.data.delete(key);
            }
        });
    }

    log(...args) {
        this.logger(moment().toISOString(), ...args);
    }
}

module.exports = {
    RedisConnection,
    RedisServer
};

if (require.main === module) {
    let options = {};
    let args = process.argv.slice(2);
    while (args.length) {
        let option = args.shift();
        if (option === '-p' || option === '--port') options.port = parseInt(args.shift(), 10);
        else if (option === '-a' || option === '--address') options.address = args.shift();
        else if (option === '-i' || option === '--save-interval') options.saveInterval = parseInt(args.shift(), 10);
        else if (option === '-n' || option === '--no-save') options.saveInterval = 0;
        else if (option === '-e' || option === '--expire-interval') options.expireInterval = parseInt(args.shift(), 10);
        else if (option === '-f' || option === '--filename') options.filename = args.shift();
        else options.filename = option;
    }
    new RedisServer(options);
}

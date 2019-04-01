const WebSocket = require('ws');
const { Serialize } = require('eosjs');
const fetch = require('node-fetch');
const { TextDecoder, TextEncoder } = require('text-encoding');
const abiAbi = require('./node_modules/eosjs/src/abi.abi.json');
const zlib = require('zlib');
const commander = require('commander');

const abiTypes = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), abiAbi);

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function toJsonNoBin(x) {
    return JSON.stringify(x, (k, v) => {
        if (v instanceof Uint8Array)
            return "...";
        return v;
    }, 4)
}

class Connection {
    constructor({ socketAddress, receivedAbi, receivedBlock }) {
        this.receivedAbi = receivedAbi;
        this.receivedBlock = receivedBlock;

        this.abi = null;
        this.types = null;
        this.tables = new Map;
        this.blocksQueue = [];
        this.inProcessBlocks = false;

        this.ws = new WebSocket(socketAddress, { perMessageDeflate: false,maxPayload: 2048*1024*1024 });
        this.ws.on('message', data => this.onMessage(data));
    }

    serialize(type, value) {
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder });
        Serialize.getType(this.types, type).serialize(buffer, value);
        return buffer.asUint8Array();
    }

    deserialize(type, array) {
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
        let result = Serialize.getType(this.types, type).deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: true }));
        if (buffer.readPos != array.length)
            throw new Error('oops: ' + type); // todo: remove check
        // {
        //     console.log(result.actions[0].authorization[0].actor);
        //     //console.log('oops: ' + type);
        // }
        return result;
    }

    toJsonUnpackTransaction(x) {
        return JSON.stringify(x, (k, v) => {
            if (k === 'trx' && Array.isArray(v) && v[0] === 'packed_transaction') {
                const pt = v[1];
                let packed_trx = pt.packed_trx;
                if (pt.compression === 0)
                    packed_trx = this.deserialize('transaction', packed_trx);
                else if (pt.compression === 1)
                    packed_trx = this.deserialize('transaction', zlib.unzipSync(packed_trx));
                return { ...pt, packed_trx };
            }
            if (k === 'packed_trx' && v instanceof Uint8Array)
                return this.deserialize('transaction', v);
            if (v instanceof Uint8Array)
                return `(${v.length} bytes)`;
            return v;
        }, 4)
    }

    send(request) {
        this.ws.send(this.serialize('request', request));
    }

    onMessage(data) {
        try {
            if (!this.abi) {
                this.abi = JSON.parse(data);
                this.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), this.abi);
                for (const table of this.abi.tables)
                    this.tables.set(table.name, table.type);
                if (this.receivedAbi)
                    this.receivedAbi();
            } else {
                const [type, response] = this.deserialize('result', data);
                this[type](response);
            }
        } catch (e) {
            console.log(e);
            process.exit(1);
        }
    }

    requestStatus() {
        this.send(['get_status_request_v0', {}]);
    }

    requestBlocks(requestArgs) {
        this.send(['get_blocks_request_v0', {
            start_block_num: 21289343,//16899370
            end_block_num: 0xffffffff,
            max_messages_in_flight: 5,
            have_positions: [],
            irreversible_only: false,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
            ...requestArgs
        }]);
    }

    get_status_result_v0(response) {
        console.log(response);
    }

    get_blocks_result_v0(response) {
        this.blocksQueue.push(response);
        this.processBlocks();
    }

    async processBlocks() {
        if (this.inProcessBlocks)
            return;
        this.inProcessBlocks = true;
        while (this.blocksQueue.length) {
            let response = this.blocksQueue.shift();
            this.send(['get_blocks_ack_request_v0', { num_messages: 1 }]);
            //console.log(response.this_block.block_num);
            if (response.this_block)
                process.stdout.write(`\r Block: ${numberWithCommas(response.this_block.block_num)}`);


            let block, traces = [], deltas = [];
            if (response.block && response.block.length)
                block = this.deserialize('signed_block', response.block);
            if (response.traces && response.traces.length)
                traces = this.deserialize('transaction_trace[]', zlib.unzipSync(response.traces));
            if (response.deltas && response.deltas.length)
                deltas = this.deserialize('table_delta[]', zlib.unzipSync(response.deltas));
            await this.receivedBlock(response, block, traces, deltas);
            //return;
        }
        this.inProcessBlocks = false;

    }

    forEachRow(delta, f) {
        const type = this.tables.get(delta.name);
        for (let row of delta.rows) {
            let data;
            try {
                data = this.deserialize(type, row.data);
            } catch (e) {
                console.error(e);
            }
            if (data)
                f(row.present, data[1]);
        }
    }

    dumpDelta(delta, extra) {
        this.forEachRow(delta, (present, data) => {
            //console.log(this.toJsonUnpackTransaction({ ...extra, present, data }));
        });
    }
} // Connection

class Monitor {
    constructor({ socketAddress }) {
        this.accounts = new Map;
        this.tableIds = new Map;

        this.connection = new Connection({
            socketAddress,
            receivedAbi: () => this.connection.requestBlocks({
                fetch_block: true,
                fetch_traces: true,
                fetch_deltas: true,
            }),
            receivedBlock: async (response, block, traces, deltas) => {
                if (!response.this_block)
                    return;
                if (!(response.this_block.block_num % 100))
                    //console.log(`block ${numberWithCommas(response.this_block.block_num)}`)
                if (block)
                    //console.log(this.connection.toJsonUnpackTransaction(block));
                if (traces.length)
                    //console.log(toJsonNoBin(traces));
                for (let [_, delta] of deltas)
                    //if (delta.name === 'resource_limits_config')
                    this.connection.dumpDelta(delta, { name: delta.name, block_num: response.this_block.block_num });
                for (let [_, delta] of deltas)
                    if (this[delta.name])
                        this[delta.name](response.this_block.block_num, delta);
            }
        });
    }

    getAccount(name) {
        const account = this.accounts.get(name);
        if (!account || !account.rawAbi.length)
            throw new Error('no abi for ' + name);
        if (!account.abi)
            account.abi = abiTypes.get("abi_def").deserialize(new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array: account.rawAbi }));
        if (!account.types)
            account.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), account.abi);
        return account;
    }

    deserializeTable(name, tableName, array) {
        const account = this.getAccount(name);
        const typeName = account.abi.tables.find(t => t.name == tableName).type;
        const type = Serialize.getType(account.types, typeName);
        const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
        return type.deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: false }));
    }

    account(blockNum, delta) {
        this.connection.forEachRow(delta, (present, data) => {
            if (present && data.abi.length) {
                //console.log(`block: ${blockNum} ${data.name}: set abi`);
                this.accounts.set(data.name, { rawAbi: data.abi });
            } else if (this.accounts.has(data.name)) {
                //console.log(`block: ${blockNum} ${data.name}: clear abi`);
                this.accounts.delete(data.name);
            }
        });
    }

    contract_row(blockNum, delta) {
        // this.connection.forEachRow(delta, (present, data) => {
        //     if (data.code !== 'eosio.token' && data.table !== 'accounts' || data.scope !== 'eosio')
        //         return;
        //     let content = this.deserializeTable(data.code, data.table, data.value);
        //     console.log(`block: ${blockNum} present: ${present} code:${data.code} scope:${data.scope} table:${data.table} table_payer:${data.payer} payer:${data.payer} primary_key:${data.primary_key}  ${JSON.stringify(content)}`);
        // });
    }

    generated_transaction(blockNum, delta) {
        this.connection.forEachRow(delta, (present, data) => {
            if (data.sender === '.............')
                return;
            //console.log('generated_transaction')
            //console.log(this.connection.toJsonUnpackTransaction({ present, ...data }));
        });
    }
} // Monitor

commander
    .option('-d, --delete-schema', 'Delete schema')
    .option('-c, --create-schema', 'Create schema and tables')
    .option('-i, --irreversible-only', 'Only follow irreversible')
    .option('-s, --schema [name]', 'Schema name', 'chain')
    .option('-a, --socket-address [addr]', 'Socket address', 'ws://178.128.115.181:8080/')
    .parse(process.argv);

const monitor = new Monitor(commander);

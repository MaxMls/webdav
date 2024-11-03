import { createClient, type WebDAVClient } from 'webdav';
import path from 'path';
import { createReadStream, readdirSync, statSync } from 'fs';
import archiver from "archiver";
import { Writable } from 'stream';

interface WebDAVClientWithLimit extends WebDAVClient {
    index: number;
    minSize: number;
    maxSize: number;
}

const webdavClients: WebDAVClientWithLimit[] = [];
let index = 0;
const indexFormat = () => index === 0 ? '' : `_${index + 1}`
const winToLinux = path => path.replace(/\\/g, '/').replace(/^([a-zA-Z]):/, '');

const DIRECTORY_PATH = process.env.DIRECTORY_PATH;
if (!DIRECTORY_PATH) {
    console.error('Error: DIRECTORY_PATH config is not defined in the environment variables.');
    process.exit(1); // Exit the process with an error code
}

if (!process.env.PACK_FILES_SMALLER_THAN) {
    console.error('Error: PACK_FILES_SMALLER_THAN config is not defined in the environment variables.');
    process.exit(1); // Exit the process with an error code
}
const PACK_FILES_SMALLER_THAN = parseInt(process.env.PACK_FILES_SMALLER_THAN);

if (!process.env.PACK_SIZE) {
    console.error('Error: PACK_SIZE config is not defined in the environment variables.');
    process.exit(1); // Exit the process with an error code
}
const PACK_SIZE = parseInt(process.env.PACK_SIZE);

while (process.env[`WEBDAV_URL${indexFormat()}`]) {
    const url = process.env[`WEBDAV_URL${indexFormat()}`];
    const username = process.env[`WEBDAV_USERNAME${indexFormat()}`];
    const password = process.env[`WEBDAV_PASSWORD${indexFormat()}`];
    const minSize = process.env[`WEBDAV_FILE_MIN_SIZE${indexFormat()}`];
    const maxSize = process.env[`WEBDAV_FILE_MAX_SIZE${indexFormat()}`];

    if (!url) {
        console.error('Error: WEBDAV_URL is not defined in the environment variables.');
        process.exit(1); // Exit the process with an error code
    }

    if (!username) {
        console.error('Error: WEBDAV_USERNAME is not defined in the environment variables.');
        process.exit(1); // Exit the process with an error code
    }

    if (!password) {
        console.error('Error: WEBDAV_PASSWORD is not defined in the environment variables.');
        process.exit(1); // Exit the process with an error code
    }

    if (!minSize) {
        console.error('Error: WEBDAV_FILE_MIN_SIZE is not defined in the environment variables.');
        process.exit(1); // Exit the process with an error code
    }

    if (!maxSize) {
        console.error('Error: WEBDAV_FILE_MAX_SIZE is not defined in the environment variables.');
        process.exit(1); // Exit the process with an error code
    }

    const client = createClient(url, { username, password }) as WebDAVClientWithLimit;
    client.index = index;
    client.minSize = parseInt(minSize);
    client.maxSize = parseInt(maxSize);
    webdavClients.push(client);
    index++;
}
const client = (size = 0) => {
    return webdavClients[1]
    const clients = webdavClients.filter(client => size >= client.minSize && size <= client.maxSize);
    const index = Math.floor(Math.random() * clients.length);
    const client = clients[index];

    if (!client) {
        throw new Error('No webdav clients available');
    }
    return client
};

if (!webdavClients.length) {
    console.error('Error: WEBDAV config is not defined in the environment variables.');
    process.exit(1); // Exit the process with an error code
}

let totalBytesBatched = 0;
let totalBytesUploaded = 0;
let startTime: number;
let totalSize = 0;

const ignore = ['/users/macuser/Library']
const packs: Pack[] = [];
let dirs = {};
const uploadQueue: string[] = [];
const filesSizes = {}

interface Pack {
    files: string[],
    size: number,
    name: string
}

const visited = new Set();
function readDirectoryRecursively(dir: string, isRoot = true) {
    if (ignore.includes(dir)) return;
    if (visited.has(dir)) return; // avoid circular loop
    visited.add(dir);

    const pushLastPack = () => {
        uploadQueue.push(packs[packs.length - 1].name);
        filesSizes[packs[packs.length - 1].name] = packs[packs.length - 1].size;
    }

    readdirSync(dir).forEach(file => {
        file = winToLinux(file);
        const filePath = path.join(dir, file);
        const stats = statSync(filePath);
        if (stats.isDirectory()) {
            try {
                readDirectoryRecursively(filePath, false);
            } catch (error) {
                // console.warn(error);
                console.log('Error reading directory:', filePath);
                return
            }
        } else {
            if (stats.size < PACK_FILES_SMALLER_THAN) {
                const randomString = () => Math.random().toString(36).substring(2, 5)
                if (packs.length === 0) {
                    packs.push({ name: `pack_0.${randomString()}.zip`, files: [], size: 0 });
                }
                let pack = packs[packs.length - 1]

                if (pack.size + stats.size > PACK_SIZE) {
                    pushLastPack();

                    packs.push({ files: [], size: 0, name: `pack_${packs.length}.${randomString()}.zip` });
                }

                pack = packs[packs.length - 1];
                pack.files.push(filePath);
                pack.size += stats.size;
            } else {
                filesSizes[filePath] = stats.size;
                totalSize += stats.size;
                uploadQueue.push(filePath);
            }

        }
    });


    if (isRoot && packs.length > 0) {
        pushLastPack()
    }
}



function createDirectory(dir: string) {
    if (dir === '.' || dir === '/' || dir.endsWith(':\\')) {
        return Promise.resolve();
    }

    if (!dirs[dir]) {
        console.log({ dir });

        dirs[dir] = (async () => {
            await createDirectory(path.dirname(dir));
            await client(0).createDirectory(winToLinux(dir));
        })()
    }

    return dirs[dir];
}

const tick = {
    update: () => { },
    error: (e) => {
        console.log('error')
    }
};

/**
 * @param {archiver.Archiver} archive
 */
async function toBuffer(archive) {
    // create writable and save chunks
    /** @type {Uint8Array[]} */
    const chunks = [];
    const writable = new Writable();
    writable._write = (chunk, encoding, callback) => {
      // save to array to concatenate later
      chunks.push(chunk);
      callback();
    };
  
    // pipe to writable
    archive.pipe(writable);
    await archive.finalize();
  
    // once done, concatenate chunks
    return Buffer.concat(chunks);
  }

const createArchive = async (pack: Pack) => {
    const archive = archiver.create('zip', { zlib: { level: 1 } });
    pack.files.forEach(file => {
        archive.file(file, { name: file })
    })

    return toBuffer(archive);
}

async function uploadFile(file: string) {
    let data;

    const pack = packs.find(pack => pack.name === file)
    if (pack) {
        data = await createArchive(pack);
    } else {
        data = createReadStream(file);
    }

    // create directory if it doesn't exist
    // console.log(`Start ${filePath} (${fileStats.size} bytes)`);
    // console.log(`Creating directory ${dir}`);
    const dir = path.dirname(file);
    await createDirectory(dir);
    // console.log(`Uploading ${filePath} (${fileStats.size} bytes)`);
    const clientInstanse = client(filesSizes[file]);
    try {
        const res = await clientInstanse.putFileContents(winToLinux(file), data, { overwrite: true });

        if (res !== true) {
            console.error('Error uploading file:', file, res);
        }
    } catch (error) {
        console.log(`client: ${clientInstanse.index}`, `file: ${file}`, `error: ${error}`);
        throw error
    }
}

async function manageUploads() {
    startTime = Date.now();
    // 100mb and 100 files same time in queue
    // minimum 1 file in queue
    let batch = new Set<string>();

    let minQueuedBytes = 1000000; // 1mb
    let maxQueuedBytes = 20000000; // 10mb
    let minBatchSize = 2;
    let maxBatchSize = 30;

    let limitQueuedBytes = maxQueuedBytes;
    let limitBatchSize = maxBatchSize;

    let dateStart = new Date().getTime();
    const interval = setInterval(() => {
        const speed = (totalBytesUploaded) / ((new Date().getTime() - dateStart) / 1000);

        console.log(`Uploaded ${totalBytesUploaded / 1000000} mb in ${Date.now() - startTime}ms. Speed: ${speed / 1000000} mb/s.`);

        const queuedBytes = totalBytesBatched - totalBytesUploaded;
        console.log(`Batched ${batch.size} files, size: ${queuedBytes / 1000000} mb limitBatchSize: ${limitBatchSize}, limitQueuedBytes: ${limitQueuedBytes / 1000000} mb, totalSize: ${totalSize / 1000000} mb, queue files: ${uploadQueue.length}`);
        console.log(Array.from(batch).map(file => `${path.basename(file)} (${filesSizes[file] / 1000000} mb)`).join('\n'));
    }, 1000);


    while (true) {
        const queuedBytes = totalBytesBatched - totalBytesUploaded;

        // 10mb
        if (uploadQueue.length !== 0 && (queuedBytes < limitQueuedBytes && batch.size < limitBatchSize || batch.size === 0)) {
            const file = uploadQueue.shift();
            if (!file) throw new Error("uploadQueue is empty");

            const fileSize = filesSizes[file];
            totalBytesBatched += fileSize;
            batch.add(file);
            uploadFile(file).then(() => {
                batch.delete(file);
                limitBatchSize = Math.min(maxBatchSize, limitBatchSize * 1.2);
                limitQueuedBytes = Math.min(maxQueuedBytes, limitQueuedBytes * 1.2);

                totalBytesUploaded += fileSize;
                tick.update();
            }).catch((e) => {
                console.error(e);
                batch.delete(file);

                limitBatchSize = Math.max(minBatchSize, limitBatchSize / 1.1);
                limitQueuedBytes = Math.max(minQueuedBytes, limitQueuedBytes / 1.1);

                totalBytesBatched -= fileSize;
                uploadQueue.unshift(file);
            });
        } else {
            await new Promise<void>((resolve, reject) => {
                tick.update = resolve;
                tick.error = reject;
            });
        }

        if (batch.size === 0 && uploadQueue.length === 0) {
            break
        }
    }

    console.log('Done');
    clearInterval(interval);
}

readDirectoryRecursively(DIRECTORY_PATH);
console.log(dirs);


manageUploads().catch(console.error);

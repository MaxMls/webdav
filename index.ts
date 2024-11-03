import { createClient } from 'webdav';
import fs from 'fs/promises';
import path from 'path';
import { readdirSync, statSync } from 'fs';

const WEBDAV_URL = process.env.WEBDAV_URL;
if (!WEBDAV_URL) {
	console.error('Error: WEBDAV_URL is not defined in the environment variables.');
	process.exit(1); // Exit the process with an error code
}

const WEBDAV_USERNAME = process.env.WEBDAV_USERNAME;
if (!WEBDAV_USERNAME) {
	console.error('Error: WEBDAV_USERNAME is not defined in the environment variables.');
	process.exit(1); // Exit the process with an error code
}

const WEBDAV_PASSWORD = process.env.WEBDAV_PASSWORD;
if (!WEBDAV_PASSWORD) {
	console.error('Error: WEBDAV_PASSWORD is not defined in the environment variables.');
	process.exit(1); // Exit the process with an error code
}

const client = createClient(WEBDAV_URL, {
    username: WEBDAV_USERNAME,
    password: WEBDAV_PASSWORD,
});

const directoryPath = '/users/macuser';
const uploadQueue: string[] = [];
/** размер данных переданных на отправку */
let totalBytesBatched = 0;
/** размер переданных на сервер данных  */
let totalBytesUploaded = 0;
let startTime: number;

const filesSizes = {}
const visited = new Set();

const ignore = ['/users/macuser/Library']

function readDirectoryRecursively(dir: string) {
    if(ignore.includes(dir)) return;
    if (visited.has(dir)) return; // avoid circular loop
    visited.add(dir);

    readdirSync(dir).forEach(file => {
        const filePath = path.join(dir, file);
        const stats = statSync(filePath);
        if (stats.isDirectory()) {
            try {
                readDirectoryRecursively(filePath);
            } catch (error) {
                // console.warn(error);
                console.log('Error reading directory:', filePath);
                return
            }
        } else {
            filesSizes[filePath] = stats.size;
            uploadQueue.push(filePath);
        }
    });
}

let dirs = {};

function createDirectory(dir: string) {
    if (dir === '.' || dir === '/') {
        return Promise.resolve();
    }

    if (!dirs[dir]) {
        console.log({dir});

        dirs[dir] = (async ()=>{
            await createDirectory(path.dirname(dir));
            await client.createDirectory(dir);
        })()
    }

    return dirs[dir];
}

const tick = {
    update: ()=>{}, 
error: (e)=>{
    console.log('error')
}};

async function uploadFile(filePath: string) {
    const fileStream = await fs.readFile(filePath);

    // create directory if it doesn't exist
    const dir = path.dirname(filePath);
    // console.log(`Start ${filePath} (${fileStats.size} bytes)`);
    // console.log(`Creating directory ${dir}`);
    await createDirectory(dir);
    // console.log(`Uploading ${filePath} (${fileStats.size} bytes)`);
    const res = await client.putFileContents(filePath, fileStream, {overwrite: true});
   
    if (res !== true) {
        console.error('Error uploading file:', filePath, res);
    }

}

async function manageUploads() {
    startTime = Date.now();
    // 100mb and 100 files same time in queue
    // minimum 1 file in queue
    let batch = new Set<string>();

    let minQueuedBytes = 1000000; // 1mb
    let maxQueuedBytes = 1000000000; // 1000mb
    let minBatchSize = 1;
    let maxBatchSize = 300;

    let limitQueuedBytes = maxQueuedBytes;
    let limitBatchSize = maxBatchSize;

    let dateStart = new Date().getTime();
    const interval = setInterval(() => {
        const speed = (totalBytesUploaded) / ((new Date().getTime() - dateStart) / 1000);
    
        console.log(`Uploaded ${totalBytesUploaded / 1000000} mb in ${Date.now() - startTime}ms. Speed: ${speed / 1000000} mb/s.`);
        
        const queuedBytes = totalBytesBatched - totalBytesUploaded;
        console.log(`Batched ${batch.size} files, size: ${queuedBytes / 1000000} mb limitBatchSize: ${limitBatchSize}, limitQueuedBytes: ${limitQueuedBytes / 1000000} mb`);
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
                
                limitBatchSize = Math.max(minBatchSize, limitBatchSize/1.1);
                limitQueuedBytes =  Math.max(minQueuedBytes, limitQueuedBytes/1.1);

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

readDirectoryRecursively(directoryPath);
console.log(dirs);


manageUploads().catch(console.error);

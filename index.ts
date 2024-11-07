import path from 'path';
import { createReadStream, ReadStream } from 'fs';
import { open } from 'lmdb'; // or require
import { type File, winToLinux } from './common.ts';
import { WebDAV } from './webdav.ts';
import { fileStateDB } from './db.ts';
import { fileZipperIterator } from './zipper.ts';

const DIRECTORY_PATH = process.env.DIRECTORY_PATH;
if (!DIRECTORY_PATH) {
    console.error('Error: DIRECTORY_PATH config is not defined in the environment variables.');
    process.exit(1); // Exit the process with an error code
}

class Upload {
    private minQueuedBytes: number;
    private maxQueuedBytes: number;
    private minBatchSize: number;
    private maxBatchSize: number;
    private limitQueuedBytes: any;
    private limitBatchSize: any;
    private dateStart: number;
    private failedFiles: File[];
    private interval: NodeJS.Timeout;
    private batch: Set<File>;
    private dir: string;
    private dirs: Record<string, Promise<void>>;
    private startTime: number;
    private webdav: WebDAV;
    private totalBytesUploaded: number;

    constructor(dir: string) {
        this.dir = dir;
        this.dirs = {};
        this.minQueuedBytes = 1000000; // 1mb;
        this.maxQueuedBytes = 20000000; // 10mb;
        this.minBatchSize = 2;
        this.maxBatchSize = 30;

        this.limitQueuedBytes = this.maxQueuedBytes;
        this.limitBatchSize = this.maxBatchSize;

        this.dateStart = new Date().getTime();
        this.failedFiles = [];

        this.interval = setInterval(this.log, 1000);
        this.batch = new Set();

        this.startTime = Date.now();

        this.webdav = new WebDAV();

        this.totalBytesUploaded = 0;
    }


    private createDirectory = async (dir: string) => {
        if (dir === '.' || dir === '/' || dir.endsWith(':\\')) {
            return Promise.resolve();
        }

        if (!this.dirs[dir]) {
            // console.log({ dir });

            this.dirs[dir] = (async () => {
                await this.createDirectory(path.dirname(dir));
                try {
                    await this.webdav.client(0).createDirectory(winToLinux(dir));
                } catch (error) {
                    console.warn(error);
                }
            })()
        }

        return this.dirs[dir];
    }

    private uploadFile = async (file: File) => {
        const clientInstanse = this.webdav.client(0);
        file.status = 'check if file exists';
        const exists = await clientInstanse.exists(winToLinux(file.file));
        if (exists) {
            file.status = 'done uploading file exists';
            return;
        }

        let data: ReadStream | Buffer;
        if (file.data) {
            file.status = 'data fetching';
            data = await file.data()
            file.status = 'done data fetching';
            file.size = data.byteLength;
        } else {
            data = createReadStream(file.file);
        }

        file.status = 'creating directory';
        const dir = path.dirname(file.file);
        await this.createDirectory(dir);
        file.status = 'done creating directory';

        try {
            file.status = 'uploading';
            const res = await clientInstanse.putFileContents(winToLinux(file.file), data, {
                overwrite: false,
                contentLength: file.size,
                onUploadProgress: (event) => {
                    file.status = 'uploading ' + (event.loaded / file.size) * 100 + '%'
                }
            });
            file.status = 'done uploading';

            if (res !== true) {
                file.status = 'file already exists';
                console.warn('File already exists:', file, res);
            } else{
                this.totalBytesUploaded += file.size;
            }
        } catch (error) {
            file.status = 'error uploading: ' + error;
            console.log(`client: ${clientInstanse.index}`, `file: ${file}`, `error: ${error}`);
            throw error
        }
    }

    private queueIterator = async function* (dir: string): AsyncIterableIterator<File> {
        for await (const file of fileZipperIterator(dir)) {
            yield file
        }

        for (const file of this.failedFiles) {
            yield file
        }
    }

    private fileProcessingBatched = async (file: File) => {
        this.batch.add(file);
        file.promise = this.fileProcessing(file);
        await file.promise;
        this.batch.delete(file);
        delete file.promise;
    }

    private fileProcessing = async (file: File) => {
        try {
            await this.uploadFile(file);
            // console.log(`(${file.size / 1000000} MB) ${file.file}`);

            this.limitBatchSize = Math.min(this.maxBatchSize, this.limitBatchSize * 1.2);
            this.limitQueuedBytes = Math.min(this.maxQueuedBytes, this.limitQueuedBytes * 1.2);

            await fileStateDB.put(file.file, 'done');
        } catch (error) {
            console.error(error);
            this.limitBatchSize = Math.max(this.minBatchSize, this.limitBatchSize / 1.1);
            this.limitQueuedBytes = Math.max(this.minQueuedBytes, this.limitQueuedBytes / 1.1);
            this.failedFiles.push(file);
        }
    }

    private log = () => {
        const speed = (this.totalBytesUploaded) / ((new Date().getTime() - this.dateStart) / 1000);

        console.log(`Uploaded ${this.totalBytesUploaded / 1000000} mb in ${Date.now() - this.startTime}ms. Speed: ${speed / 1000000} mb/s.`);

        const batch = Array.from(this.batch);
        const queuedBytes = Array.from(this.batch).reduce((acc, file) => acc + file.size, 0);
        console.log(`Batched ${batch.length} files, size: ${queuedBytes / 1000000} mb limitBatchSize: ${this.limitBatchSize}, limitQueuedBytes: ${this.limitQueuedBytes / 1000000} mb`);
        console.log(batch.map(file => `${path.basename(file.file)} (${file.size / 1000000} mb) ${file.status}`).join('\n'));
    }

    start = async () => {
        this.interval = setInterval(this.log, 2000);

        for await (const file of this.queueIterator(this.dir)) {

            // wait untill file suits given conditions
            while (true) {
                if (this.batch.size === 0) {
                    break;
                }
                const batchSize = Array.from(this.batch).reduce((acc, file) => acc + file.size, 0);
                if (batchSize < this.limitQueuedBytes && this.batch.size < this.limitBatchSize) {
                    break;
                }
                await Promise.any(Array.from(this.batch).map(file => file.promise));
            }
            this.fileProcessingBatched(file);
        }
        
        console.log('All files uploaded.');
        clearInterval(this.interval);
    }

}
const upload = new Upload(DIRECTORY_PATH);
upload.start();

import path from 'path';
import { createReadStream } from 'fs';
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
    private batch: Set<Promise<void>>;
    private dir: string;
    private dirs: Record<string, Promise<void>>;
    private totalBytesBatched: number;
    private totalBytesUploaded: number;
    private startTime: number;
    private webdav: WebDAV;

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
        this.batch = new Set<Promise<void>>();


        this.totalBytesBatched = 0;
        this.totalBytesUploaded = 0;
        this.startTime = Date.now();

        this.webdav = new WebDAV();
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
        const data = file.data ?? createReadStream(file.file);

        const dir = path.dirname(file.file);
        await this.createDirectory(dir);

        const clientInstanse = this.webdav.client(file.size);
        try {
            const res = await clientInstanse.putFileContents(winToLinux(file.file), data, { overwrite: true, contentLength: file.size });

            if (res !== true) {
                console.error('Error uploading file:', file, res);
            }
        } catch (error) {
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
        const promise = this.fileProcessing(file);
        this.batch.add(promise);
        await promise;
        this.batch.delete(promise);
    }

    private fileProcessing = async (file: File) => {
        

        try {
            this.totalBytesBatched += file.size;
            await this.uploadFile(file);
            // console.log(`(${file.size / 1000000} MB) ${file.file}`);

            this.limitBatchSize = Math.min(this.maxBatchSize, this.limitBatchSize * 1.2);
            this.limitQueuedBytes = Math.min(this.maxQueuedBytes, this.limitQueuedBytes * 1.2);

            this.totalBytesUploaded += file.size;

            await fileStateDB.put(file.file, 'done');
        } catch (error) {
            console.error(error);
            this.limitBatchSize = Math.max(this.minBatchSize, this.limitBatchSize / 1.1);
            this.limitQueuedBytes = Math.max(this.minQueuedBytes, this.limitQueuedBytes / 1.1);
            this.totalBytesBatched -= file.size;

            this.failedFiles.push(file);
        }
    }

    private log = () => {
        const speed = (this.totalBytesUploaded) / ((new Date().getTime() - this.dateStart) / 1000);

        console.log(`Uploaded ${this.totalBytesUploaded / 1000000} mb in ${Date.now() - this.startTime}ms. Speed: ${speed / 1000000} mb/s.`);

        const queuedBytes = this.totalBytesBatched - this.totalBytesUploaded;
        console.log(`Batched ${this.batch.size} files, size: ${queuedBytes / 1000000} mb limitBatchSize: ${this.limitBatchSize}, limitQueuedBytes: ${this.limitQueuedBytes / 1000000} mb`);
        // console.log(Array.from(batch).map(file => `${path.basename(file)} (${filesSizes[file] / 1000000} mb)`).join('\n'));
    }


    start = async () => {
        this.interval = setInterval(this.log, 1000);

        for await (const file of this.queueIterator(this.dir)) {

            // wait untill file suits given conditions
            while (true) {
                if (this.batch.size === 0) {
                    break;
                }

                if (this.totalBytesBatched - this.totalBytesUploaded < this.limitQueuedBytes && this.batch.size < this.limitBatchSize) {
                    break;
                }
                await Promise.any(this.batch);
            }
            this.fileProcessingBatched(file);
        }
        clearInterval(this.interval);
    }

}
const upload = new Upload(DIRECTORY_PATH);
upload.start();

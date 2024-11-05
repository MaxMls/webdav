import archiver from "archiver";
import { Writable } from 'stream';
import { fileStateDB } from "./db.ts";
import { type File, readDirectoryRecursivelyAsyncIterator } from "./common.ts";

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



const toBuffer = async (archive) => {
    const chunks: Uint8Array[] = [];
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

export const createArchive = async (pack: string[]) => {
    const archive = archiver.create('zip', { zlib: { level: 1 } });
    pack.forEach(file => {
        archive.file(file, { name: file })
    })

    return toBuffer(archive);
}


class Pack {
    files: string[];
    size: number;
    name: string;
    index: number;
    constructor(index: number = 0) {
        this.index = index;
        this.files = [];
        this.size = 0;
        this.name = `pack.${index}.${Math.random().toString(36).substring(2, 11)}.zip`;
    }

    file = async () => {
        //const buffer = await createArchive(this.files);
        // return { file: this.name, size: buffer.byteLength, data: buffer }

        return { file: this.name, size: this.size, data: () => createArchive(this.files) }
    }

    push = async (file: File) => {
        this.files.push(file.file);
        await fileStateDB.put(file.file, this.name);
        this.size += file.size;
    }

    newPack = () => new Pack(this.index + 1)
}


export const fileZipperIterator = async function* (dir: string): AsyncIterableIterator<File> {
    let pack: Pack = new Pack();

    const pushPack = async function* () {
        if (!pack.files) {
            return
        }

        yield pack.file();
        pack = pack.newPack();
    }

    for await (const file of readDirectoryRecursivelyAsyncIterator(dir)) {
        if (file.size < PACK_FILES_SMALLER_THAN) {
            if (pack.size + file.size > PACK_SIZE) {
                yield* pushPack();
            }

            pack.push(file);
        } else {
            yield file;
        }
    }

    yield* pushPack();
}


import { readdir, stat } from 'fs/promises';
import path from "path";
import { fileStateDB } from './db.ts';

const ignore = ['/users/macuser/Library']

export const winToLinux = path => path.replace(/\\/g, '/').replace(/^([a-zA-Z]):/, '');


export interface File {
    file: string,
    size: number,
    data?: Buffer
}

export const readDirectoryRecursivelyAsyncIterator = async function* (dir: string, visited = new Set<string>()): AsyncIterableIterator<File> {
    if (ignore.includes(dir)) {
        return;
    }
    if (visited.has(dir)) {
        return;
    }// avoid circular loop
    visited.add(dir);

    const files = await readdir(dir);

    for (let file of files) {
        file = winToLinux(file);

        const filePath = path.join(dir, file);
        const stats = await stat(filePath);
        if (stats.isDirectory()) {
            yield* readDirectoryRecursivelyAsyncIterator(filePath, visited);
        } else {
            const resFile = { file: filePath, size: stats.size }
            let state: string = await fileStateDB.get(resFile.file);
            if (state && state !== 'done') {
                state = await fileStateDB.get(state);
            }
            if (state) {
                console.log('File already uploaded:', resFile.file);
                return;
            }

            yield resFile;
        }
    };
};

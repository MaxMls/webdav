import { createClient, type WebDAVClient } from "webdav";

const DRY_RUN = false;

interface WebDAVClientWithLimit extends WebDAVClient {
    index: number;
    minSize: number;
    maxSize: number;
}


export class WebDAV {
    webdavClients: WebDAVClientWithLimit[];

    constructor() {
        this.webdavClients = [];
        let index = 0;
        const indexFormat = () => index === 0 ? '' : `_${index + 1}`;

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
            this.webdavClients.push(client);
            index++;
        }

        if (!this.webdavClients.length) {
            console.error('Error: WEBDAV config is not defined in the environment variables.');
            process.exit(1); // Exit the process with an error code
        }
    }

    client = (size = 0) => {
        if (DRY_RUN) {
            return {
                putFileContents: (file) => {
                    console.log(file);
                    return true
                },
                createDirectory: (dir) => {
                    console.log(dir);
                    return true
                },
                index: -1
            }
        }

        return this.webdavClients[1]
        const clients = this.webdavClients.filter(client => size >= client.minSize && size <= client.maxSize);
        const index = Math.floor(Math.random() * clients.length);
        const client = clients[index];

        if (!client) {
            throw new Error('No webdav clients available');
        }
        return client
    }



}
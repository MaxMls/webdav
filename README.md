# Script for Uploading Data to a WebDAV Server

**Optimized for faster uploads**

## Main Features:
1. Small files are packaged into archives to speed up transmission (archives maintain the original project hierarchy).
2. The number of concurrently processed files is dynamically adjusted to maximize transfer speed.
3. Data can be transferred to multiple WebDAV servers simultaneously.

## How to Use: (NodeJS >=22)
1. Create a `.env` configuration (see example: `.env copy`).
2. Run `npm i`.
3. Run `npm run start`.
4. The server will shut down upon completion of the upload.

## Implementation Details:
1. There is no capability for resuming uploads or restarting after the script is interrupted.
2. Folder merging is not fully developed (uploading to a clean server is recommended).

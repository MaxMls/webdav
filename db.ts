import { open } from "lmdb";

export const fileStateDB = open({ path: 'fileStateDB.lmdb' });

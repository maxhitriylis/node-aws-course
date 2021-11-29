import fs from 'fs';
import split2 from 'split2';
import { pipeline } from 'stream/promises';
import { Transform, Writable, TransformCallback } from 'stream';
import csv from 'csvtojson';

const PATH_TO_CSV = './nodejs-hw1-ex1.csv';
const OUTPUT_FILE = './csvOutput.txt';

let header: null | string = null;

const fakeInsertMany = (data: string[]) => {
  return new Promise<string[]>((resolve) => {
    setTimeout(() => {
      resolve(data);
    }, 1000);
  });
};

const writableOutputStream = fs.createWriteStream(OUTPUT_FILE);
const readFromFileStream = fs.createReadStream(PATH_TO_CSV);

class WriteToFIleAndDBStream extends Writable {
  private buffer: string[] = [];

  constructor() {
    super();
  }

  private async writeToDB() {
    try {
      const response = await fakeInsertMany(this.buffer);
      this.buffer = [];
      console.log('Data was written successfully', response);
    } catch (error) {
      console.log('Error writing to database', error);
    }
  }

  async _write(chunk: Buffer, _: any, callback: TransformCallback) {
    const dataRow = chunk.toString();
    const canReadNext = writableOutputStream.write(chunk);
    if (!canReadNext) {
      readFromFileStream.pause();
      writableOutputStream.once('drain', () => readFromFileStream.resume());
    }
    if (this.buffer.length >= 4) {
      await this.writeToDB();
    }
    this.buffer.push(dataRow);
    callback();
  }
}

const transformStream = new Transform({
  transform: async (chunk: Buffer, _, callback) => {
    if (!header) {
      header = chunk.toString();
      callback();
    } else {
      const line = chunk.toString();
      const fromCsvToArray = await csv().fromString(`${header}\n${line}`);
      const stringified = `${JSON.stringify(fromCsvToArray[0])}\n`;
      callback(null, stringified);
    }
  },
});

const convertFromCsv = async () => {
  try {
    await pipeline(readFromFileStream, split2(), transformStream, new WriteToFIleAndDBStream());
    console.log('Pipeline succeeded');
  } catch (error) {
    if (error instanceof Error) {
      console.error('Pipeline failed', error.message);
    }
  }
};

convertFromCsv();
